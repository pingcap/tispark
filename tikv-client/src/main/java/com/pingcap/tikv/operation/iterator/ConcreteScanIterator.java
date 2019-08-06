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

package com.pingcap.tikv.operation.iterator;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.RegionStoreClient.RegionStoreClientBuilder;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;
import java.util.Objects;
import org.apache.log4j.Logger;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.kvproto.Metapb;

public class ConcreteScanIterator extends ScanIterator {
  private final long version;
  private final Logger logger = Logger.getLogger(ConcreteScanIterator.class);

  public ConcreteScanIterator(
      TiConfiguration conf, RegionStoreClientBuilder builder, ByteString startKey, long version) {
    // Passing endKey as ByteString.EMPTY means that endKey is +INF by default,
    this(conf, builder, startKey, ByteString.EMPTY, version);
  }

  public ConcreteScanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      ByteString endKey,
      long version) {
    super(conf, builder, startKey, endKey, Integer.MAX_VALUE);
    this.version = version;
  }

  TiRegion loadCurrentRegionToCache() throws Exception {
    TiRegion region;
    try (RegionStoreClient client = builder.build(startKey)) {
      region = client.getRegion();
      BackOffer backOffer = ConcreteBackOffer.newScannerNextMaxBackOff();
      currentCache = client.scan(backOffer, startKey, version);
      return region;
    }
  }

  private ByteString resolveCurrentLock(Kvrpcpb.KvPair current) {
    logger.warn(String.format("resolve current key error %s", current.getError().toString()));
    Pair<TiRegion, Metapb.Store> pair =
        builder.getRegionManager().getRegionStorePairByKey(startKey);
    TiRegion region = pair.first;
    Metapb.Store store = pair.second;
    BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
    try (RegionStoreClient client = builder.build(region, store)) {
      return client.get(backOffer, current.getKey(), version);
    } catch (Exception e) {
      throw new KeyException(current.getError());
    }
  }

  @Override
  public KvPair next() {
    KvPair current;
    // continue when cache is empty but not null
    for (current = getCurrent(); currentCache != null && current == null; current = getCurrent()) {
      if (cacheLoadFails()) {
        return null;
      }
    }

    Objects.requireNonNull(current, "current kv pair cannot be null");
    if (current.hasError()) {
      ByteString val = resolveCurrentLock(current);
      current = KvPair.newBuilder().setKey(current.getKey()).setValue(val).build();
    }

    return current;
  }
}
