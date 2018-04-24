package com.pingcap.tikv.operation.iterator;

import static com.pingcap.tikv.meta.TiDAGRequest.PushDownType.STREAMING;

import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.DAGRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.exception.RegionTaskException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.meta.TiDAGRequest.PushDownType;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.RangeSplitter;

import java.util.*;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DAGIterator<T> extends CoprocessIterator<T> {
  private ExecutorCompletionService<Iterator<SelectResponse>> streamingService;
  private ExecutorCompletionService<SelectResponse> dagService;
  private SelectResponse response;
  private static final Logger logger = LoggerFactory.getLogger(DAGIterator.class.getName());

  private Iterator<SelectResponse> responseIterator;

  private final PushDownType pushDownType;

  DAGIterator(DAGRequest req,
              List<RangeSplitter.RegionTask> regionTasks,
              TiSession session,
              SchemaInfer infer,
              PushDownType pushDownType) {
    super(req, regionTasks, session, infer);
    this.pushDownType = pushDownType;
    switch (pushDownType) {
      case NORMAL:
        dagService = new ExecutorCompletionService<>(session.getThreadPoolForTableScan());
        break;
      case STREAMING:
        streamingService = new ExecutorCompletionService<>(session.getThreadPoolForTableScan());
        break;
    }
    submitTasks();
  }

  @Override
  void submitTasks() {
    for (RangeSplitter.RegionTask task : regionTasks) {
      switch (pushDownType) {
        case STREAMING:
          streamingService.submit(() -> processByStreaming(task));
          break;
        case NORMAL:
          dagService.submit(() -> process(task));
          break;
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (eof) {
      return false;
    }

    while (chunkList == null ||
        chunkIndex >= chunkList.size() ||
        dataInput.available() <= 0
        ) {
      // First we check if our chunk list has remaining chunk
      if (tryAdvanceChunkIndex()) {
        createDataInputReader();
      }
      // If not, check next region/response
      else if (pushDownType == STREAMING) {
        if (!advanceNextResponse() &&
            !readNextRegionChunks()) {
          return false;
        }
      } else if (!readNextRegionChunks()) {
        return false;
      }
    }

    return true;
  }


  private boolean hasMoreResponse() {
    switch (pushDownType) {
      case STREAMING:
        return responseIterator != null && responseIterator.hasNext();
      case NORMAL:
        return response != null;
    }

    throw new IllegalArgumentException("Invalid push down type:" + pushDownType);
  }

  private boolean advanceNextResponse() {
    if (!hasMoreResponse()) {
      return false;
    }

    switch (pushDownType) {
      case STREAMING:
        chunkList = responseIterator.next().getChunksList();
        break;
      case NORMAL:
        chunkList = response.getChunksList();
        break;
    }

    if (chunkList == null || chunkList.isEmpty()) {
      return false;
    }

    chunkIndex = 0;
    createDataInputReader();
    return true;
  }

  private boolean readNextRegionChunks() {
    if (eof ||
        regionTasks == null ||
        taskIndex >= regionTasks.size()) {
      return false;
    }

    try {
      switch (pushDownType) {
        case STREAMING:
          responseIterator = streamingService.take().get();
          break;
        case NORMAL:
          response = dagService.take().get();
          break;
      }

    } catch (Exception e) {
      throw new TiClientInternalException("Error reading region:", e);
    }

    taskIndex++;
    return advanceNextResponse();
  }

  private SelectResponse process(RangeSplitter.RegionTask regionTask) {
    Queue<RangeSplitter.RegionTask> remainTasks = new ArrayDeque<>();
    Queue<SelectResponse> responseQueue = new ArrayDeque<>();
    remainTasks.add(regionTask);
    BackOffer backOffer = ConcreteBackOffer.newCopNextMaxBackOff();
    // In case of one region task spilt into several others, we ues a queue to properly handle all
    // the remaining tasks.
    while (!remainTasks.isEmpty()) {
      RangeSplitter.RegionTask task = remainTasks.poll();
      if (task == null) continue;
      List<Coprocessor.KeyRange> ranges = task.getRanges();
      TiRegion region = task.getRegion();
      Metapb.Store store = task.getStore();
      Metapb.Store.Builder builder = Metapb.Store.newBuilder()
          .setAddress(store.getAddress())
          .setState(store.getState())
          .setStateValue(store.getStateValue());
      for (int i = 0; i < store.getLabelsCount(); i++) {
        builder.setLabels(i, store.getLabels(i));
      }
      if (ThreadLocalRandom.current().nextInt(2) == 0) {
        int id = 233333333;
        logger.warn("Using random store id for mock request" + id);
        builder.setId(id);
      } else {
        builder.setId(store.getId());
      }
      store = builder.build();

      try {
        RegionStoreClient client = RegionStoreClient.create(region, store, session);
        Collection<RangeSplitter.RegionTask> tasks =
            client.coprocess(backOffer, dagRequest, ranges, responseQueue);
        if (tasks != null) {
          remainTasks.addAll(tasks);
        }
      } catch (Throwable e) {
        // Handle region task failed
        logger.error("Process region tasks failed, remain " + remainTasks.size() + " tasks not executed due to", e);
        // Rethrow to upper levels
        eof = true;
        throw new RegionTaskException("Handle region task failed:", e);
      }
    }

    // Add all chunks to the final result
    List<Chunk> resultChunk = new ArrayList<>();
    while (!responseQueue.isEmpty()) {
      SelectResponse response = responseQueue.poll();
      if (response != null) {
        resultChunk.addAll(response.getChunksList());
      }
    }

    return SelectResponse.newBuilder()
        .addAllChunks(resultChunk)
        .build();
  }

  private Iterator<SelectResponse> processByStreaming(RangeSplitter.RegionTask regionTask) {
    List<Coprocessor.KeyRange> ranges = regionTask.getRanges();
    TiRegion region = regionTask.getRegion();
    Metapb.Store store = regionTask.getStore();

    RegionStoreClient client;
    try {
      client = RegionStoreClient.create(region, store, session);
      Iterator<SelectResponse> responseIterator = client.coprocessStreaming(dagRequest, ranges);
      if (responseIterator == null) {
        eof = true;
        return null;
      }
      return responseIterator;
    } catch (Exception e) {
      // TODO: Fix stale error handling in streaming
      // see:https://github.com/pingcap/tikv-client-lib-java/pull/149
      throw new TiClientInternalException("Error Closing Store client.", e);
    }
  }
}