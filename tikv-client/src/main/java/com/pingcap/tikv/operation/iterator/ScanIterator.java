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

import static java.util.Objects.requireNonNull;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import java.util.Iterator;
import java.util.List;

public abstract class ScanIterator implements Iterator<Kvrpcpb.KvPair> {
  protected final TiSession session;
  protected final RegionManager regionCache;

  protected List<Kvrpcpb.KvPair> currentCache;
  protected ByteString startKey;
  protected int index = -1;
  protected int limit;
  protected boolean endOfScan = false;

  protected Key endKey;
  protected boolean hasEndKey;
  protected boolean lastBatch = false;

  ScanIterator(ByteString startKey, ByteString endKey, int limit, TiSession session) {
    this.startKey = requireNonNull(startKey, "start key is null");
    if (startKey.isEmpty()) {
      throw new IllegalArgumentException("start key cannot be empty");
    }
    this.endKey = Key.toRawKey(requireNonNull(endKey, "end key is null"));
    this.hasEndKey = !endKey.equals(ByteString.EMPTY);
    this.limit = limit;
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
      // currentCache is null means no keys found, whereas currentCache is empty means no values
      // found
      // the difference lies in whether to continue scanning, because chances are that the same key
      // is
      // split in another region because of pending entries, region split, e.t.c.
      // See https://github.com/pingcap/tispark/issues/393 for details
      if (currentCache == null) {
        return true;
      }
      index = 0;
      Key lastKey = Key.EMPTY;
      // Session should be single-threaded itself
      // so that we don't worry about conf change in the middle
      // of a transaction. Otherwise below code might lose data
      if (currentCache.size() < session.getConf().getScanBatchSize()) {
        startKey = curRegionEndKey;
      } else {
        // Start new scan from exact next key in current region
        lastKey = Key.toRawKey(currentCache.get(currentCache.size() - 1).getKey());
        startKey = lastKey.next().toByteString();
      }
      // notify last batch if lastKey is greater than or equal to endKey
      if (hasEndKey && lastKey.compareTo(endKey) >= 0) {
        lastBatch = true;
        startKey = null;
      }
    } catch (Exception e) {
      throw new TiClientInternalException("Error scanning data from region.", e);
    }
    return false;
  }

  boolean isCacheDrained() {
    return currentCache == null || limit <= 0 || index >= currentCache.size() || index == -1;
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
      --limit;
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
