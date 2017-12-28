/*
 * Copyright 2017 PingCAP, Inc.
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

import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.kvproto.Kvrpcpb.IsolationLevel;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.Comparables;
import com.pingcap.tikv.util.KeyRangeUtils;
import com.pingcap.tikv.util.Pair;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class ScanIterator implements Iterator<Kvrpcpb.KvPair> {
  private final Range scanRange;
  private final int batchSize;
  protected final TiSession session;
  private final RegionManager regionCache;
  protected final long version;
  private final Kvrpcpb.IsolationLevel isolationLevel = IsolationLevel.RC;

  private List<Kvrpcpb.KvPair> currentCache;
  protected ByteString startKey;
  protected int index = -1;
  private boolean endOfRegion = false;

  public ScanIterator(
      ByteString startKey,
      int batchSize,
      KeyRange range,
      TiSession session,
      RegionManager rm,
      long version) {
    this.startKey = startKey;
    this.batchSize = batchSize;
    this.scanRange = KeyRangeUtils.toRange(range);
    this.session = session;
    this.regionCache = rm;
    this.version = version;
  }

  private boolean loadCache() {
    if (endOfRegion) return false;

    Pair<TiRegion, Metapb.Store> pair = regionCache.getRegionStorePairByKey(startKey);
    TiRegion region = pair.first;
    Metapb.Store store = pair.second;
    try (RegionStoreClient client = RegionStoreClient.create(region, store, session)) {
      currentCache = client.scan(startKey, version);
      if (currentCache == null || currentCache.size() == 0) {
        return false;
      }
      index = 0;
      // Session should be single-threaded itself
      // so that we don't worry about conf change in the middle
      // of a transaction. Otherwise below code might lose data
      if (currentCache.size() < batchSize) {
        // Current region done, start new batch from next region
        startKey = region.getEndKey();
        if (startKey.size() == 0 || !contains(startKey)) {
          return false;
        }
      } else {
        // Start new scan from exact next key in current region
        ByteString lastKey = currentCache.get(currentCache.size() - 1).getKey();
        startKey = KeyUtils.getNextKeyInByteOrder(lastKey);
      }
    } catch (Exception e) {
      throw new TiClientInternalException("Error Closing Store client.", e);
    }
    return true;
  }

  private boolean cacheDrain() {
    return currentCache == null
        || index >= currentCache.size()
        || index == -1;
  }

  @Override
  public boolean hasNext() {
    if (cacheDrain() && endOfRegion) {
      return false;
    }
    if (cacheDrain()) {
      if (!loadCache()) {
        endOfRegion = true;
      }
    }
    if (!contains(currentCache.get(index).getKey())) {
      endOfRegion = true;
      return false;
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  private boolean contains(ByteString key) {
    return scanRange.contains(Comparables.wrap(key));
  }

  private Kvrpcpb.KvPair getCurrent() {
    if (cacheDrain() && endOfRegion) {
      throw new NoSuchElementException();
    }
    if (index < currentCache.size()) {
      Kvrpcpb.KvPair kv = currentCache.get(index++);
      if (!contains(kv.getKey())) {
        endOfRegion = true;
        throw new NoSuchElementException();
      }
      return kv;
    }
    return null;
  }

  @Override
  public Kvrpcpb.KvPair next() {
    Kvrpcpb.KvPair kv = getCurrent();
    if (kv == null) {
      // cache drained
      if (!loadCache()) {
        return null;
      }
      return getCurrent();
    }
    return kv;
  }
}
