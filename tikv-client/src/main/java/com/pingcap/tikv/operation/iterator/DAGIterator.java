package com.pingcap.tikv.operation.iterator;

import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.DAGRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.exception.GrpcRegionStaleException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.meta.TiDAGRequest.PushDownType;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.RangeSplitter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorCompletionService;

import static com.pingcap.tikv.meta.TiDAGRequest.PushDownType.STREAMING;

public abstract class DAGIterator<T> extends CoprocessIterator<T> {
  private ExecutorCompletionService<Iterator<SelectResponse>> streamingService;
  private ExecutorCompletionService<SelectResponse> dagService;
  private SelectResponse response;

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
    List<Coprocessor.KeyRange> ranges = regionTask.getRanges();
    TiRegion region = regionTask.getRegion();
    Metapb.Store store = regionTask.getStore();

    RegionStoreClient client;
    try {
      client = RegionStoreClient.create(region, store, session);
      SelectResponse response = client.coprocess(dagRequest, ranges);
      if (response == null) {
        eof = true;
        return null;
      }
      return response;
    } catch (GrpcRegionStaleException e) {
      List<Chunk> resultChunk = new ArrayList<>();
      List<RangeSplitter.RegionTask> splitTasks = RangeSplitter
          .newSplitter(session.getRegionManager())
          .splitRangeByRegion(ranges);

      for (RangeSplitter.RegionTask t : splitTasks) {
        SelectResponse resFromCurTask = process(t);
        if (resFromCurTask != null) {
          resultChunk.addAll(resFromCurTask.getChunksList());
        }
      }

      return SelectResponse.newBuilder()
          .addAllChunks(resultChunk)
          .build();
    }
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