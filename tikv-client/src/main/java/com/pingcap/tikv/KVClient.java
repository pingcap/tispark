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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.pingcap.tikv;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiKVException;
import com.pingcap.tikv.operation.iterator.ConcreteScanIterator;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.RegionStoreClient.*;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffFunction;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.KvPair;

public class KVClient implements AutoCloseable {
  private final RegionStoreClientBuilder clientBuilder;
  private final TiConfiguration conf;
  private final ExecutorService executorService;
  private static final Logger logger = Logger.getLogger(KVClient.class);

  private static final int BATCH_GET_SIZE = 16 * 1024;

  public KVClient(TiConfiguration conf, RegionStoreClientBuilder clientBuilder) {
    Objects.requireNonNull(conf, "conf is null");
    Objects.requireNonNull(clientBuilder, "clientBuilder is null");
    this.conf = conf;
    this.clientBuilder = clientBuilder;
    // TODO: ExecutorService executors =
    // Executors.newFixedThreadPool(conf.getKVClientConcurrency());
    executorService = Executors.newFixedThreadPool(20);
  }

  @Override
  public void close() {
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  /**
   * Get a raw key-value pair from TiKV if key exists
   *
   * @param key raw key
   * @return a ByteString value if key exists, ByteString.EMPTY if key does not exist
   */
  public ByteString get(ByteString key, long version) throws GrpcException {
    BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
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
   * @param keys keys
   */
  public List<KvPair> batchGet(List<ByteString> keys, long version) {
    return batchGet(ConcreteBackOffer.newBatchGetMaxBackOff(), keys, version);
  }

  private List<KvPair> batchGet(BackOffer backOffer, List<ByteString> keys, long version) {
    Set<ByteString> set = new HashSet<>(keys);
    return batchGet(backOffer, set, version);
  }

  private List<KvPair> batchGet(BackOffer backOffer, Set<ByteString> keys, long version) {
    Map<TiRegion, List<ByteString>> groupKeys = groupKeysByRegion(keys);
    List<Batch> batches = new ArrayList<>();

    for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
      appendBatches(batches, entry.getKey(), entry.getValue(), BATCH_GET_SIZE);
    }
    return sendBatchGet(backOffer, batches, version);
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @return list of key-value pairs in range
   */
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey, long version) {
    Iterator<Kvrpcpb.KvPair> iterator =
        scanIterator(conf, clientBuilder, startKey, endKey, version);
    List<Kvrpcpb.KvPair> result = new ArrayList<>();
    iterator.forEachRemaining(result::add);
    return result;
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param limit limit of key-value pairs
   * @return list of key-value pairs in range
   */
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, int limit) {
    Iterator<Kvrpcpb.KvPair> iterator = scanIterator(conf, clientBuilder, startKey, limit);
    List<Kvrpcpb.KvPair> result = new ArrayList<>();
    iterator.forEachRemaining(result::add);
    return result;
  }

  /** A Batch containing the region and a list of keys to send */
  private static final class Batch {
    private final TiRegion region;
    private final List<ByteString> keys;

    Batch(TiRegion region, List<ByteString> keys) {
      this.region = region;
      this.keys = keys;
    }
  }

  /**
   * Append batch to list and split them according to batch limit
   *
   * @param batches a grouped batch
   * @param region region
   * @param keys keys
   * @param limit batch max limit
   */
  private void appendBatches(
      List<Batch> batches, TiRegion region, List<ByteString> keys, int limit) {
    List<ByteString> tmpKeys = new ArrayList<>();
    for (int i = 0; i < keys.size(); i++) {
      if (i >= limit) {
        batches.add(new Batch(region, tmpKeys));
        tmpKeys.clear();
      }
      tmpKeys.add(keys.get(i));
    }
    if (!tmpKeys.isEmpty()) {
      batches.add(new Batch(region, tmpKeys));
    }
  }

  /**
   * Group by list of keys according to its region
   *
   * @param keys keys
   * @return a mapping of keys and their region
   */
  private Map<TiRegion, List<ByteString>> groupKeysByRegion(Set<ByteString> keys) {
    return keys.stream()
        .collect(Collectors.groupingBy(clientBuilder.getRegionManager()::getRegionByKey));
  }

  /**
   * Send batchPut request concurrently
   *
   * @param backOffer current backOffer
   * @param batches list of batch to send
   */
  private List<KvPair> sendBatchGet(BackOffer backOffer, List<Batch> batches, long version) {
    ExecutorCompletionService<List<KvPair>> completionService =
        new ExecutorCompletionService<>(executorService);
    for (Batch batch : batches) {
      completionService.submit(
          () -> {
            RegionStoreClient client = clientBuilder.build(batch.region);
            BackOffer singleBatchBackOffer = ConcreteBackOffer.create(backOffer);
            List<ByteString> keys = batch.keys;
            try {
              return client.batchGet(singleBatchBackOffer, keys, version);
            } catch (final TiKVException e) {
              // TODO: any elegant way to re-split the ranges if fails?
              singleBatchBackOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
              logger.warn("ReSplitting ranges for BatchPutRequest");
              // recursive calls
              return batchGet(singleBatchBackOffer, batch.keys, version);
            }
          });
    }
    try {
      List<KvPair> result = new ArrayList<>();
      for (int i = 0; i < batches.size(); i++) {
        result.addAll(
            completionService.take().get(BackOffer.BATCH_GET_MAX_BACKOFF, TimeUnit.SECONDS));
      }
      return result;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TiKVException("Current thread interrupted.", e);
    } catch (TimeoutException e) {
      throw new TiKVException("TimeOut Exceeded for current operation. ", e);
    } catch (ExecutionException e) {
      throw new TiKVException("Execution exception met.", e);
    }
  }

  private Iterator<Kvrpcpb.KvPair> scanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      ByteString endKey,
      long version) {
    return new ConcreteScanIterator(conf, builder, startKey, endKey, version);
  }

  private Iterator<Kvrpcpb.KvPair> scanIterator(
      TiConfiguration conf, RegionStoreClientBuilder builder, ByteString startKey, int limit) {
    return new ConcreteScanIterator(conf, builder, startKey, limit);
  }
}
