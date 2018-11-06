/*
 * Copyright 2018 PingCAP, Inc.
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
 */

package com.pingcap.tikv.operation.iterator;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;

public class ConcreteScanIterator extends ScanIterator {
  private final long version;

  public ConcreteScanIterator(
      ByteString startKey,
      TiSession session,
      long version) {
    // Passing endKey as ByteString.EMPTY means that endKey is +INF by default,
    super(startKey, ByteString.EMPTY, Integer.MAX_VALUE, session);
    this.version = version;
  }

  TiRegion loadCurrentRegionToCache() throws Exception {
    Pair<TiRegion, Metapb.Store> pair = regionCache.getRegionStorePairByKey(startKey);
    TiRegion region = pair.first;
    Metapb.Store store = pair.second;
    try (RegionStoreClient client = RegionStoreClient.create(region, store, session)) {
      BackOffer backOffer = ConcreteBackOffer.newScannerNextMaxBackOff();
      currentCache = client.scan(backOffer, startKey, version);
      return region;
    }
  }
}
