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

import static java.util.Objects.requireNonNull;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.region.RegionStoreClient.RegionStoreClientBuilder;
import com.pingcap.tikv.region.TiRegion;
import java.util.Iterator;
import java.util.List;
import org.tikv.kvproto.Kvrpcpb;

public abstract class ScanIterator implements Iterator<Kvrpcpb.KvPair> {
  protected final TiConfiguration conf;
  protected final RegionStoreClientBuilder builder;
  protected List<Kvrpcpb.KvPair> currentCache;
  protected ByteString startKey;
  protected int index = -1;
  protected int limit;
  protected boolean endOfScan = false;

  protected Key endKey;
  protected boolean hasEndKey;
  protected boolean lastBatch = false;

  ScanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      ByteString endKey,
      int limit) {
    this.startKey = requireNonNull(startKey, "start key is null");
    if (startKey.isEmpty()) {
      throw new IllegalArgumentException("start key cannot be empty");
    }
    this.endKey = Key.toRawKey(requireNonNull(endKey, "end key is null"));
    this.hasEndKey = !endKey.equals(ByteString.EMPTY);
    this.limit = limit;
    this.conf = conf;
    this.builder = builder;
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
      // found. The difference lies in whether to continue scanning, because chances are that
      // an empty region exists due to deletion, region split, e.t.c.
      // See https://github.com/pingcap/tispark/issues/393 for details
      if (currentCache == null) {
        return true;
      }
      index = 0;
      Key lastKey = Key.EMPTY;
      // Session should be single-threaded itself
      // so that we don't worry about conf change in the middle
      // of a transaction. Otherwise below code might lose data
      if (currentCache.size() < conf.getScanBatchSize()) {
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

  Kvrpcpb.KvPair getCurrent() {
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
    Kvrpcpb.KvPair current;
    // continue when cache is empty but not null
    for (current = getCurrent(); currentCache != null && current == null; current = getCurrent()) {
      if (cacheLoadFails()) {
        return null;
      }
    }
    return current;
  }
}
