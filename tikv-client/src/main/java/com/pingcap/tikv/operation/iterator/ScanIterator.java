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
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;

import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public abstract class ScanIterator implements Iterator<Kvrpcpb.KvPair> {
  protected final TiSession session;
  protected final RegionManager regionCache;

  protected List<Kvrpcpb.KvPair> currentCache;
  protected ByteString startKey;
  protected int index = -1;
  protected boolean endOfScan = false;

  protected Key endKey;
  protected boolean lastBatch = false;

  ScanIterator(ByteString startKey, TiSession session) {
    this.startKey = requireNonNull(startKey, "start key is null");
    if (startKey.isEmpty()) {
      throw new IllegalArgumentException("start key cannot be empty");
    }
    this.session = session;
    this.regionCache = session.getRegionManager();
  }

  abstract TiRegion loadCurrentRegionToCache() throws Exception;

  // return true if current cache is not loaded or empty
  boolean cacheLoadFails() {
    if (endOfScan || lastBatch) {
      return true;
    }
    if (startKey.isEmpty()) {
      return true;
    }
    try {
      TiRegion region = loadCurrentRegionToCache();
      ByteString curRegionEndKey = region.getEndKey();
      if (currentCache == null) {
        return true;
      }
      index = 0;
      Key lastKey = Key.EMPTY;
      if (currentCache.size() < session.getConf().getScanBatchSize()) {
        startKey = curRegionEndKey;
      } else {
        lastKey = Key.toRawKey(currentCache.get(currentCache.size() - 1).getKey());
        startKey = lastKey.next().toByteString();
      }
      if (endKey != null && lastKey.compareTo(endKey) > 0) {
        lastBatch = true;
        startKey = null;
      }
    } catch (Exception e) {
      throw new TiClientInternalException("Error scanning data from region.", e);
    }
    return false;
  }

  boolean isCacheDrained() {
    return currentCache == null
        || index >= currentCache.size()
        || index == -1;
  }

  @Override
  public boolean hasNext() {
    if (isCacheDrained() && cacheLoadFails()) {
      endOfScan = true;
      return false;
    }
    return true;
  }

  private Kvrpcpb.KvPair getCurrent() {
    if (isCacheDrained()) {
      return null;
    }
    if (index < currentCache.size()) {
      return currentCache.get(index++);
    }
    return null;
  }

  @Override
  public Kvrpcpb.KvPair next() {
    Kvrpcpb.KvPair kv = getCurrent();
    if (kv == null) {
      // cache drained
      if (cacheLoadFails()) {
        return null;
      }
      return getCurrent();
    }
    return kv;
  }
}
