/*
 * Copyright 2020 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.operation.iterator;

import static com.pingcap.tikv.meta.TiDAGRequest.PushDownType.STREAMING;

import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.DAGRequest;
import com.pingcap.tidb.tipb.EncodeType;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.ClientSession;
import com.pingcap.tikv.TiFlashClient;
import com.pingcap.tikv.meta.TiDAGRequest.PushDownType;
import com.pingcap.tikv.operation.SchemaInfer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorCompletionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.exception.RegionTaskException;
import org.tikv.common.exception.TiClientInternalException;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.region.TiStoreType;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.RangeSplitter;
import org.tikv.common.util.RangeSplitter.RegionTask;
import org.tikv.kvproto.Coprocessor;

public abstract class DAGIterator<T> extends CoprocessorIterator<T> {
  private static final Logger logger = LoggerFactory.getLogger(DAGIterator.class.getName());
  private final PushDownType pushDownType;
  private final TiStoreType storeType;
  private final long startTs;
  protected EncodeType encodeType;
  private ExecutorCompletionService<Iterator<SelectResponse>> streamingService;
  private ExecutorCompletionService<SelectResponse> dagService;
  private SelectResponse response;
  private Iterator<SelectResponse> responseIterator;

  DAGIterator(
      DAGRequest req,
      List<RegionTask> regionTasks,
      ClientSession clientSession,
      SchemaInfer infer,
      PushDownType pushDownType,
      TiStoreType storeType,
      long startTs) {
    super(req, regionTasks, clientSession, infer);
    this.pushDownType = pushDownType;
    this.storeType = storeType;
    this.startTs = startTs;
    switch (pushDownType) {
      case NORMAL:
        dagService =
            new ExecutorCompletionService<>(
                clientSession.getTiKVSession().getThreadPoolForTableScan());
        break;
      case STREAMING:
        streamingService =
            new ExecutorCompletionService<>(
                clientSession.getTiKVSession().getThreadPoolForTableScan());
        break;
    }
    submitTasks();
  }

  @Override
  void submitTasks() {
    for (RegionTask task : regionTasks) {
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

    while (chunkList == null || chunkIndex >= chunkList.size() || dataInput.available() <= 0) {
      // First we check if our chunk list has remaining chunk
      if (tryAdvanceChunkIndex()) {
        createDataInputReader();
      }
      // If not, check next region/response
      else if (pushDownType == STREAMING) {
        if (!advanceNextResponse() && !readNextRegionChunks()) {
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
        SelectResponse resp = responseIterator.next();
        chunkList = resp.getChunksList();
        this.encodeType = resp.getEncodeType();
        break;
      case NORMAL:
        chunkList = response.getChunksList();
        this.encodeType = this.response.getEncodeType();
        break;
    }

    if (chunkList == null || chunkList.isEmpty()) {
      return false;
    }

    chunkIndex = 0;
    createDataInputReader();
    return true;
  }

  /**
   * chunk maybe empty while there is still data transmitting from TiKV. In this case, {@code
   * readNextRegionChunks} cannot just returns false because the iterator thinks there is no data to
   * process. This while loop ensures we can drain all possible data transmitting from TiKV.
   *
   * @return
   */
  private boolean readNextRegionChunks() {
    while (hasNextRegionTask()) {
      if (doReadNextRegionChunks()) {
        return true;
      }
    }
    return false;
  }

  private boolean hasNextRegionTask() {
    return !(eof || regionTasks == null || taskIndex >= regionTasks.size());
  }

  private boolean doReadNextRegionChunks() {
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

  private SelectResponse process(RegionTask regionTask) {
    Queue<RegionTask> remainTasks = new ArrayDeque<>();
    Queue<SelectResponse> responseQueue = new ArrayDeque<>();
    remainTasks.add(regionTask);
    BackOffer backOffer = ConcreteBackOffer.newCopNextMaxBackOff();
    HashSet<Long> resolvedLocks = new HashSet<>();
    BackOffer storeUnreachableBackOffer = ConcreteBackOffer.newCustomBackOff(60 * 1000);

    // In case of one region task spilt into several others, we ues a queue to properly handle all
    // the remaining tasks.
    while (!remainTasks.isEmpty()) {
      RegionTask task = remainTasks.poll();
      if (task == null) {
        continue;
      }
      List<Coprocessor.KeyRange> ranges = task.getRanges();
      TiRegion region = task.getRegion();
      TiStore store = task.getStore();

      try {
        if (store == null || !store.isReachable()) {
          storeUnreachableBackOffer.doBackOffWithMaxSleep(
              BackOffFunction.BackOffFuncType.BoServerBusy,
              2000,
              new TiClientInternalException("retry timeout: store is null or unreachable"));
          if (store == null) {
            logger.warn("TiKV store is null, invalid cache and retry");
          } else {
            logger.warn(
                "TiKV store " + store.getAddress() + " is unreachable, invalid cache and retry");
          }
          clientSession.getTiKVSession().getRegionManager().invalidateRegion(region);
          try {
            remainTasks.addAll(
                RangeSplitter.newSplitter(clientSession.getTiKVSession().getRegionManager())
                    .splitRangeByRegion(ranges, storeType));
          } catch (Exception e) {
            // If the pd is switching leader, the region invalid exception will be thrown. In this
            // case, we can retry with the original task.
            logger.warn("split range by region error, retry with the original task", e);
            remainTasks.add(task);
          }
          continue;
        }

        RegionStoreClient client =
            clientSession
                .getTiKVSession()
                .getRegionStoreClientBuilder()
                .build(region, store, storeType);
        // if mpp store is not alive, drop it and generate a new task.
        if (storeType == TiStoreType.TiFlash
            && !isMppStoreAlive(
                store.getAddress(), clientSession.getConf().getHealthCheckTimeout())) {
          logger.info("Re-splitting region task due to TiFlash is unavailable");
          remainTasks.addAll(
              RangeSplitter.newSplitter(clientSession.getTiKVSession().getRegionManager())
                  .splitRangeByRegion(ranges, storeType));
          continue;
        }

        client.addResolvedLocks(startTs, resolvedLocks);
        logger.info(
            String.format(
                "start coprocess request to %s in region %d with timeout %s",
                task.getHost(),
                region.getId(),
                client.getTimeout()));

        Collection<RegionTask> tasks =
            client.coprocess(backOffer, dagRequest, ranges, responseQueue, startTs);
        if (tasks != null) {
          remainTasks.addAll(tasks);
        }
        resolvedLocks.addAll(client.getResolvedLocks(startTs));
      } catch (Throwable e) {
        // Handle region task failed
        logger.error(
            "Process region tasks failed, remain "
                + remainTasks.size()
                + " tasks not executed due to",
            e);
        // Rethrow to upper levels
        throw new RegionTaskException("Handle region task failed:", e);
      }
    }

    // Add all chunks to the final result
    List<Chunk> resultChunk = new ArrayList<>();
    EncodeType encodeType = null;
    while (!responseQueue.isEmpty()) {
      SelectResponse response = responseQueue.poll();
      if (response != null) {
        encodeType = response.getEncodeType();
        resultChunk.addAll(response.getChunksList());
      }
    }

    return SelectResponse.newBuilder().addAllChunks(resultChunk).setEncodeType(encodeType).build();
  }

  private Iterator<SelectResponse> processByStreaming(RegionTask regionTask) {
    List<Coprocessor.KeyRange> ranges = regionTask.getRanges();
    TiRegion region = regionTask.getRegion();
    TiStore store = regionTask.getStore();

    RegionStoreClient client;
    try {
      client =
          clientSession
              .getTiKVSession()
              .getRegionStoreClientBuilder()
              .build(region, store, storeType);
      Iterator<SelectResponse> responseIterator =
          client.coprocessStreaming(dagRequest, ranges, startTs);
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

  // See https://github.com/pingcap/tispark/pull/2619 for more details
  public Boolean isMppStoreAlive(String address, int timeout) {
    try {
      Map<String, Boolean> storeStatusCache = clientSession.getStoreStatusCache();
      return storeStatusCache.computeIfAbsent(
          address,
          key ->
              TiFlashClient.isMppAlive(
                  clientSession
                      .getTiKVSession()
                      .getChannelFactory()
                      .getChannel(
                          address, clientSession.getTiKVSession().getPDClient().getHostMapping()),
                  timeout));
    } catch (Exception e) {
      throw new TiClientInternalException("Error get MppStore Status.", e);
    }
  }
}
