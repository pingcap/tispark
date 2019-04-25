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
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.log4j.Logger;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.kvproto.Metapb;

public class ScanIterator implements Iterator<Kvrpcpb.KvPair> {
  private final Logger logger = Logger.getLogger(ScanIterator.class);
  protected final TiSession session;
  private final RegionManager regionCache;
  protected final long version;

  private List<Kvrpcpb.KvPair> currentCache;
  protected ByteString startKey;
  protected int index = -1;
  private boolean endOfScan = false;

  public ScanIterator(ByteString startKey, TiSession session, long version) {
    this.startKey = requireNonNull(startKey, "start key is null");
    if (startKey.isEmpty()) {
      throw new IllegalArgumentException("start key cannot be empty");
    }
    this.session = session;
    this.regionCache = session.getRegionManager();
    this.version = version;
  }

  // return false if current cache is loaded or not empty.
  private boolean loadCache() {
    if (endOfScan) {
      return false;
    }
    if (startKey.isEmpty()) {
      return false;
    }
    Pair<TiRegion, Metapb.Store> pair = regionCache.getRegionStorePairByKey(startKey);
    TiRegion region = pair.first;
    Metapb.Store store = pair.second;
    try (RegionStoreClient client = RegionStoreClient.create(region, store, session)) {
      BackOffer backOffer = ConcreteBackOffer.newScannerNextMaxBackOff();
      currentCache = client.scan(backOffer, startKey, version);
      // currentCache is null means no keys found, whereas currentCache is empty means no values
      // found. The difference lies in whether to continue scanning, because chances are that
      // an empty region exists due to deletion, region split, e.t.c.
      // See https://github.com/pingcap/tispark/issues/393 for details
      if (currentCache == null) {
        return false;
      }
      index = 0;
      // Session should be single-threaded itself
      // so that we don't worry about conf change in the middle
      // of a transaction. Otherwise below code might lose data
      if (currentCache.size() < session.getConf().getScanBatchSize()) {
        // Current region done, start new batch from next region
        startKey = region.getEndKey();
      } else {
        // Start new scan from exact next key in current region
        Key lastKey = Key.toRawKey(currentCache.get(currentCache.size() - 1).getKey());
        startKey = lastKey.next().toByteString();
      }
    } catch (Exception e) {
      throw new TiClientInternalException("Error scanning data from region.", e);
    }
    return true;
  }

  private boolean isCacheDrained() {
    return currentCache == null || index >= currentCache.size() || index == -1;
  }

  @Override
  public boolean hasNext() {
    if (isCacheDrained() && !loadCache()) {
      endOfScan = true;
      return false;
    }
    return true;
  }

  private Kvrpcpb.KvPair getCurrent() {
    if (isCacheDrained()) {
      return null;
    }
    return currentCache.get(index++);
  }

  private ByteString resolveCurrentLock(Kvrpcpb.KvPair current) {
    logger.warn(String.format("resolve current key error %s", current.getError().toString()));
    Pair<TiRegion, Metapb.Store> pair = regionCache.getRegionStorePairByKey(startKey);
    TiRegion region = pair.first;
    Metapb.Store store = pair.second;
    BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
    try (RegionStoreClient client = RegionStoreClient.create(region, store, session)) {
      return client.get(backOffer, current.getKey(), version);
    } catch (Exception e) {
      throw new KeyException(current.getError());
    }
  }

  @Override
  public Kvrpcpb.KvPair next() {
    Kvrpcpb.KvPair current;
    // continue when cache is empty but not null
    for (current = getCurrent(); currentCache != null && current == null; current = getCurrent()) {
      if (!loadCache()) {
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
