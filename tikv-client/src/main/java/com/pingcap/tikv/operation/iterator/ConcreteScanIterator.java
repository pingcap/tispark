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

package com.pingcap.tikv.operation.iterator;

import static java.util.Objects.requireNonNull;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.exception.TiKVException;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.region.RegionManager.RegionStorePair;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.RegionStoreClient.RegionStoreClientBuilder;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffFunction;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.kvproto.Metapb;

public class ConcreteScanIterator extends ScanIterator {
  private final long version;
  private final Logger logger = LoggerFactory.getLogger(ConcreteScanIterator.class);

  public ConcreteScanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      ByteString endKey,
      long version) {
    // Passing endKey as ByteString.EMPTY means that endKey is +INF by default,
    super(conf, builder, startKey, endKey, Integer.MAX_VALUE);
    this.version = version;
  }

  @Override
  TiRegion loadCurrentRegionToCache() throws GrpcException {
    BackOffer backOffer = ConcreteBackOffer.newScannerNextMaxBackOff();
    while (true) {
      try (RegionStoreClient client = builder.build(startKey)) {
        TiRegion region = client.getRegion();
        if (limit <= 0) {
          currentCache = null;
        } else {
          try {
            int scanSize = Math.min(limit, conf.getScanBatchSize());
            currentCache = client.scan(backOffer, startKey, scanSize, version);
            // If we get region before scan, we will use region from cache which
            // may have wrong end key. This may miss some regions that split from old region.
            // Client will get the newest region during scan. So we need to
            // update region after scan.
            region = client.getRegion();
          } catch (final TiKVException e) {
            backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
            continue;
          }
        }
        return region;
      }
    }
  }

  private ByteString resolveCurrentLock(Kvrpcpb.KvPair current) {
    logger.warn(String.format("resolve current key error %s", current.getError().toString()));
    RegionStorePair regionStorePairByKey =
        builder.getRegionManager().getRegionTiKVStorePairByKey(current.getKey());
    TiRegion region = regionStorePairByKey.region;
    Metapb.Store store = regionStorePairByKey.store;
    BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
    try (RegionStoreClient client = builder.build(region, store)) {
      return client.get(backOffer, current.getKey(), version);
    } catch (Exception e) {
      throw new KeyException(current.getError());
    }
  }

  /**
   * Cache is drained when - no data extracted - scan limit was not defined - have read the last
   * index of cache - index not initialized
   *
   * @return whether cache is drained
   */
  private boolean isCacheDrained() {
    return currentCache == null || limit <= 0 || index >= currentCache.size() || index == -1;
  }

  private boolean notEndOfScan() {
    return limit > 0
        && !(processingLastBatch
            && (index >= currentCache.size()
                || Key.toRawKey(currentCache.get(index).getKey()).compareTo(endKey) >= 0));
  }

  @Override
  public boolean hasNext() {
    if (isCacheDrained() && cacheLoadFails()) {
      endOfScan = true;
      return false;
    }
    // continue when cache is empty but not null
    while (currentCache != null && currentCache.isEmpty()) {
      if (isCacheDrained() && cacheLoadFails()) {
        return false;
      }
    }
    return notEndOfScan();
  }

  private Kvrpcpb.KvPair getCurrent() {
    --limit;
    KvPair current = currentCache.get(index++);

    requireNonNull(current, "current kv pair cannot be null");
    if (current.hasError()) {
      ByteString val = resolveCurrentLock(current);
      current = KvPair.newBuilder().setKey(current.getKey()).setValue(val).build();
    }

    return current;
  }

  @Override
  public Kvrpcpb.KvPair next() {
    return getCurrent();
  }
}
