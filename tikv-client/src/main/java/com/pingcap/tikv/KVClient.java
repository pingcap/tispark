/*
 *
 * Copyright 2019 PingCAP, Inc.
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
 *
 */

package com.pingcap.tikv;

import static com.pingcap.tikv.util.ClientUtils.getBatches;
import static com.pingcap.tikv.util.ClientUtils.getTasksWithOutput;

import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiKVException;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.RegionStoreClient.RegionStoreClientBuilder;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffFunction;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.Batch;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.shade.com.google.protobuf.ByteString;

public class KVClient implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(KVClient.class);
  private static final int MAX_BATCH_LIMIT = 1024;
  private static final int BATCH_GET_SIZE = 16 * 1024;
  private final RegionStoreClientBuilder clientBuilder;
  private final TiConfiguration conf;
  private final ExecutorService batchGetThreadPool;

  public KVClient(
      TiConfiguration conf,
      RegionStoreClientBuilder clientBuilder,
      ExecutorService batchGetThreadPool) {
    Objects.requireNonNull(conf, "conf is null");
    Objects.requireNonNull(clientBuilder, "clientBuilder is null");
    this.conf = conf;
    this.clientBuilder = clientBuilder;
    this.batchGetThreadPool = batchGetThreadPool;
  }

  @Override
  public void close() {}

  /**
   * Get a key-value pair from TiKV if key exists
   *
   * @param key key
   * @return a ByteString value if key exists, ByteString.EMPTY if key does not exist
   */
  public ByteString get(ByteString key, long version) throws GrpcException {
    BackOffer backOffer =
        ConcreteBackOffer.newGetBackOff(
            clientBuilder.getRegionManager().getPDClient().getClusterId());
    while (true) {
      RegionStoreClient client = clientBuilder.build(key);
      try {
        return client.get(backOffer, key, version);
      } catch (final TiKVException e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
      }
    }
  }

  /**
   * Get a set of key-value pair by keys from TiKV
   *
   * @param backOffer
   * @param keys
   * @param version
   * @return
   * @throws GrpcException
   */
  public List<KvPair> batchGet(BackOffer backOffer, List<ByteString> keys, long version)
      throws GrpcException {
    return doSendBatchGet(backOffer, keys, version);
  }

  private List<KvPair> doSendBatchGet(BackOffer backOffer, List<ByteString> keys, long version) {
    ExecutorCompletionService<Pair<List<Batch>, List<KvPair>>> completionService =
        new ExecutorCompletionService<>(batchGetThreadPool);

    List<Batch> batches =
        getBatches(backOffer, keys, BATCH_GET_SIZE, MAX_BATCH_LIMIT, this.clientBuilder);

    // prevent stack overflow
    Queue<List<Batch>> taskQueue = new LinkedList<>();
    List<KvPair> result = new ArrayList<>();
    taskQueue.offer(batches);

    while (!taskQueue.isEmpty()) {
      List<Batch> task = taskQueue.poll();
      for (Batch batch : task) {
        completionService.submit(
            () -> doSendBatchGetInBatchesWithRetry(batch.getBackOffer(), batch, version));
      }
      result.addAll(
          getTasksWithOutput(completionService, taskQueue, task, BackOffer.RAWKV_MAX_BACKOFF));
    }

    return result;
  }

  private Pair<List<Batch>, List<KvPair>> doSendBatchGetInBatchesWithRetry(
      BackOffer backOffer, Batch batch, long version) {
    TiRegion oldRegion = batch.getRegion();
    TiRegion currentRegion =
        clientBuilder.getRegionManager().getRegionByKey(batch.getRegion().getStartKey());
    if (oldRegion.equals(currentRegion)) {
      RegionStoreClient client = clientBuilder.build(batch.getRegion());
      try {
        List<KvPair> partialResult = client.batchGet(backOffer, batch.getKeys(), version);
        return Pair.create(new ArrayList<>(), partialResult);
      } catch (final TiKVException e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        clientBuilder.getRegionManager().invalidateRegion(batch.getRegion());
        logger.debug("ReSplitting ranges for BatchGetRequest", e);
        return doRetryBatchGet(backOffer, batch);
      }
    } else {
      return doRetryBatchGet(backOffer, batch);
    }
  }

  private Pair<List<Batch>, List<KvPair>> doRetryBatchGet(BackOffer backOffer, Batch batch) {
    return Pair.create(doSendBatchGetWithRefetchRegion(backOffer, batch), new ArrayList<>());
  }

  private List<Batch> doSendBatchGetWithRefetchRegion(BackOffer backOffer, Batch batch) {
    return getBatches(backOffer, batch.getKeys(), BATCH_GET_SIZE, MAX_BATCH_LIMIT, clientBuilder);
  }
}
