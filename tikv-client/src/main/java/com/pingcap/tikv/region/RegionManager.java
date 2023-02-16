/*
 *
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
 *
 */

package com.pingcap.tikv.region;

import static com.pingcap.tikv.codec.KeyUtils.formatBytesUTF8;
import static com.pingcap.tikv.codec.KeyUtils.getEncodedKey;
import static com.pingcap.tikv.util.KeyRangeUtils.makeRange;

import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.ReadOnlyPDClient;
import com.pingcap.tikv.event.CacheInvalidateEvent;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Peer;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.Metapb.StoreState;

@SuppressWarnings("UnstableApiUsage")
public class RegionManager {
  private static final Logger logger = LoggerFactory.getLogger(RegionManager.class);
  // TODO: the region cache logic need rewrite.
  // https://github.com/pingcap/tispark/issues/1170
  private final RegionCache cache;

  private Function<CacheInvalidateEvent, Void> cacheInvalidateCallback;

  private AtomicInteger tiflashStoreIndex = new AtomicInteger(0);

  // To avoid double retrieval, we used the async version of grpc
  // When rpc not returned, instead of call again, it wait for previous one done
  public RegionManager(
      ReadOnlyPDClient pdClient, Function<CacheInvalidateEvent, Void> cacheInvalidateCallback) {
    this.cache = new RegionCache(pdClient);
    this.cacheInvalidateCallback = cacheInvalidateCallback;
  }

  public RegionManager(ReadOnlyPDClient pdClient) {
    this.cache = new RegionCache(pdClient);
    this.cacheInvalidateCallback = null;
  }

  public synchronized void setCacheInvalidateCallback(
          Function<CacheInvalidateEvent, Void> cacheInvalidateCallback) {
    this.cacheInvalidateCallback = cacheInvalidateCallback;
  }

  public Function<CacheInvalidateEvent, Void> getCacheInvalidateCallback() {
    return cacheInvalidateCallback;
  }

  public ReadOnlyPDClient getPDClient() {
    return this.cache.pdClient;
  }

  public TiRegion getRegionByKey(ByteString key) {
    return getRegionByKey(key, ConcreteBackOffer.newGetBackOff());
  }

  public TiRegion getRegionByKey(ByteString key, BackOffer backOffer) {
    return cache.getRegionByKey(key, backOffer);
  }

  public Pair<TiRegion, Store> getRegionStorePairByKey(ByteString key, BackOffer backOffer) {
    return getRegionStorePairByKey(key, TiStoreType.TiKV, backOffer);
  }

  public Pair<TiRegion, Store> getRegionStorePairByKey(ByteString key) {
    return getRegionStorePairByKey(key, TiStoreType.TiKV);
  }

  public Pair<TiRegion, Store> getRegionStorePairByKey(ByteString key, TiStoreType storeType) {
    return getRegionStorePairByKey(key, storeType, ConcreteBackOffer.newGetBackOff());
  }

  public Pair<TiRegion, Store> getRegionStorePairByKey(
      ByteString key, TiStoreType storeType, BackOffer backOffer) {
    TiRegion region = cache.getRegionByKey(key, backOffer);
    if (region == null) {
      throw new TiClientInternalException("Region not exist for key:" + formatBytesUTF8(key));
    }
    if (!region.isValid()) {
      throw new TiClientInternalException("Region invalid: " + region.toString());
    }

    Store store = null;
    if (storeType == TiStoreType.TiKV) {
      Peer leader = region.getLeader();
      store = cache.getStoreById(leader.getStoreId(), backOffer);
    } else {
      List<Store> tiflashStores = new ArrayList<>();
      for (Peer peer : region.getLearnerList()) {
        Store s = getStoreById(peer.getStoreId(), backOffer);
        for (Metapb.StoreLabel label : s.getLabelsList()) {
          if (label.getKey().equals(storeType.getLabelKey())
              && label.getValue().equals(storeType.getLabelValue())) {
            tiflashStores.add(s);
          }
        }
      }

      // select a tiflash with Round-Robin strategy
      if (tiflashStores.size() > 0) {
        store =
            tiflashStores.get(
                Math.floorMod(tiflashStoreIndex.getAndIncrement(), tiflashStores.size()));
      }

      if (store == null) {
        // clear the region cache so we may get the learner peer next time
        cache.invalidateRange(region.getStartKey(), region.getEndKey());
      }
    }

    if (store == null) {
      throw new TiClientInternalException(
          "Cannot find valid store on " + storeType + " for region " + region.toString());
    }

    return Pair.create(region, store);
  }

  public Store getStoreById(long id) {
    return getStoreById(id, ConcreteBackOffer.newGetBackOff());
  }

  public Store getStoreById(long id, BackOffer backOffer) {
    return cache.getStoreById(id, backOffer);
  }

  public void onRegionStale(TiRegion region) {
    cache.invalidateRegion(region);
  }

  public TiRegion updateLeader(TiRegion region, long storeId) {
    if (region.getLeader().getStoreId() == storeId) {
      return region;
    }
    TiRegion newRegion = region.switchPeer(storeId);
    if (cache.updateRegion(region, newRegion)) {
      return newRegion;
    }
    // failed to switch leader, possibly region is outdated, we need to drop region cache from
    // regionCache
    logger.warn("Cannot find peer when updating leader (" + region.getId() + "," + storeId + ")");
    return null;
  }

  /**
   * Clears all cache when a TiKV server does not respond
   *
   * @param region region
   */
  public void onRequestFail(TiRegion region) {
    onRequestFail(region, region.getLeader().getStoreId());
  }

  private void onRequestFail(TiRegion region, long storeId) {
    cache.invalidateRegion(region);
    cache.invalidateAllRegionForStore(storeId);
  }

  public void invalidateStore(long storeId) {
    cache.invalidateStore(storeId);
  }

  public void invalidateRegion(TiRegion region) {
    cache.invalidateRegion(region);
  }

  public void invalidateRange(ByteString startKey, ByteString endKey) {
    cache.invalidateRange(startKey,endKey);
  }

  public static class RegionCache {
    // private final Map<Long, TiRegion> regionCache;
    private final Map<Long, Store> storeCache;
    private final RangeMap<Key, TiRegion> regionCache;
    private final ReadOnlyPDClient pdClient;

    public RegionCache(ReadOnlyPDClient pdClient) {
      regionCache = TreeRangeMap.create();
      storeCache = new HashMap<>();

      this.pdClient = pdClient;
    }

    public synchronized TiRegion getRegionByKey(ByteString key, BackOffer backOffer) {
      TiRegion region = regionCache.get(getEncodedKey(key));
      if (logger.isDebugEnabled()) {
        logger.debug(
            String.format("getRegionByKey key[%s] -> Region[%s]", formatBytesUTF8(key), region));
      }

      if (region == null) {
        logger.debug("Key not found in keyToRegionIdCache:" + formatBytesUTF8(key));
        region = pdClient.getRegionByKey(backOffer, key);
        if (!putRegion(region)) {
          throw new TiClientInternalException("Invalid Region: " + region.toString());
        }
      }

      return region;
    }

    private synchronized boolean putRegion(TiRegion region) {
      if (logger.isDebugEnabled()) {
        logger.debug("putRegion: " + region);
      }
      regionCache.remove(makeRange(region.getStartKey(), region.getEndKey()));
      regionCache.put(makeRange(region.getStartKey(), region.getEndKey()), region);
      return true;
    }

    public synchronized boolean updateRegion(TiRegion expected, TiRegion region) {
      try {
        if (logger.isDebugEnabled()) {
          logger.debug(String.format("invalidateRegion ID[%s]", region.getId()));
        }
        TiRegion oldRegion = regionCache.get(getEncodedKey(region.getStartKey()));
        if (oldRegion == null || !expected.getMeta().equals(oldRegion.getMeta())) {
          return false;
        } else {
          regionCache.remove(makeRange(oldRegion.getStartKey(), oldRegion.getEndKey()));
          regionCache.put(makeRange(region.getStartKey(), region.getEndKey()), region);
          return true;
        }
      } catch (Exception ignore) {
        return false;
      }
    }

    private synchronized TiRegion getRegionFromCache(Key key) {
      return regionCache.get(key);
    }

    private synchronized void invalidateRange(ByteString startKey, ByteString endKey) {
      regionCache.remove(makeRange(startKey, endKey));
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("invalidateRange success, startKey[%s], endKey[%s]", startKey, endKey));
      }
    }

    /** Removes region associated with regionId from regionCache. */
    public synchronized void invalidateRegion(TiRegion region) {
      try {
        if (logger.isDebugEnabled()) {
          logger.debug(String.format("invalidateRegion ID[%s] start", region.getId()));
        }
        TiRegion oldRegion = regionCache.get(getEncodedKey(region.getStartKey()));
        if (oldRegion != null && oldRegion.equals(region)) {
          regionCache.remove(makeRange(region.getStartKey(), region.getEndKey()));
          if (logger.isDebugEnabled()) {
            logger.debug(String.format("invalidateRegion ID[%s] success", region.getId()));
          }
        }
      } catch (Exception e) {
        logger.warn("invalidateRegion failed", e);
      }
    }

    public synchronized void invalidateAllRegionForStore(long storeId) {
      List<TiRegion> regionToRemove = new ArrayList<>();
      for (TiRegion r : regionCache.asMapOfRanges().values()) {
        if (r.getLeader().getStoreId() == storeId) {
          if (logger.isDebugEnabled()) {
            logger.debug(String.format("invalidateAllRegionForStore Region[%s]", r));
          }
          regionToRemove.add(r);
        }
      }

      // remove region
      for (TiRegion r : regionToRemove) {
        regionCache.remove(makeRange(r.getStartKey(), r.getEndKey()));
      }
    }

    public synchronized void invalidateStore(long storeId) {
      storeCache.remove(storeId);
    }

    public synchronized Store getStoreById(long id, BackOffer backOffer) {
      try {
        Store store = storeCache.get(id);
        if (store == null) {
          store = pdClient.getStore(backOffer, id);
        }
        if (store.getState().equals(StoreState.Tombstone)) {
          return null;
        }
        storeCache.put(id, store);
        return store;
      } catch (Exception e) {
        throw new GrpcException(e);
      }
    }
  }
}
