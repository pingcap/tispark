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

import static com.pingcap.tikv.codec.KeyUtils.formatBytes;
import static com.pingcap.tikv.util.KeyRangeUtils.makeRange;

import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.ReadOnlyPDClient;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.codec.Codec;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.kvproto.Metapb.Peer;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.kvproto.Metapb.StoreState;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class RegionManager {
  private static final Logger logger = Logger.getLogger(RegionManager.class);
  private RegionCache cache;
  private final ReadOnlyPDClient pdClient;

  // To avoid double retrieval, we used the async version of grpc
  // When rpc not returned, instead of call again, it wait for previous one done
  public RegionManager(ReadOnlyPDClient pdClient) {
    this.cache = new RegionCache(pdClient);
    this.pdClient = pdClient;
  }

  public static class RegionCache {
    private final Map<Long, TiRegion> regionCache;
    private final Map<Long, Store> storeCache;
    private final RangeMap<Key, Long> keyToRegionIdCache;
    private final ReadOnlyPDClient pdClient;

    public RegionCache(ReadOnlyPDClient pdClient) {
      regionCache = new HashMap<>();
      storeCache = new HashMap<>();

      keyToRegionIdCache = TreeRangeMap.create();
      this.pdClient = pdClient;
    }

    synchronized TiRegion getRegionByKey(ByteString key) {
      Long regionId;
      regionId = keyToRegionIdCache.get(Key.toRawKey(key));
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("getRegionByKey key[%s] -> ID[%s]", formatBytes(key), regionId));
      }

      if (regionId == null) {
        logger.debug("Key not find in keyToRegionIdCache:" + formatBytes(key));
        TiRegion region = pdClient.getRegionByKey(ConcreteBackOffer.newGetBackOff(), key);
        if (!putRegion(region)) {
          throw new TiClientInternalException("Invalid Region: " + region.toString());
        }
        return region;
      }
      TiRegion region = regionCache.get(regionId);
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("getRegionByKey ID[%s] -> Region[%s]", regionId, region));
      }

      return region;
    }

    synchronized TiRegion getRegionByRawKey(ByteString key) {
      CodecDataOutput cdo = new CodecDataOutput();
      Codec.BytesCodec.writeBytes(cdo, key.toByteArray());
      return getRegionByKey(cdo.toByteString());
    }

    private synchronized boolean putRegion(TiRegion region) {
      if (logger.isDebugEnabled()) {
        logger.debug("putRegion: " + region);
      }
      regionCache.put(region.getId(), region);
      keyToRegionIdCache.put(makeRange(region.getStartKey(), region.getEndKey()), region.getId());
      return true;
    }

    private synchronized TiRegion getRegionById(long regionId) {
      TiRegion region = regionCache.get(regionId);
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("getRegionByKey ID[%s] -> Region[%s]", regionId, region));
      }
      if (region == null) {
        region = pdClient.getRegionByID(ConcreteBackOffer.newGetBackOff(), regionId);
        if (!putRegion(region)) {
          throw new TiClientInternalException("Invalid Region: " + region.toString());
        }
      }
      return region;
    }

    /** Removes region associated with regionId from regionCache. */
    public synchronized void invalidateRegion(long regionId) {
      try {
        if (logger.isDebugEnabled()) {
          logger.debug(String.format("invalidateRegion ID[%s]", regionId));
        }
        TiRegion region = regionCache.get(regionId);
        keyToRegionIdCache.remove(makeRange(region.getStartKey(), region.getEndKey()));
      } catch (Exception ignore) {
      } finally {
        regionCache.remove(regionId);
      }
    }

    public synchronized void invalidateAllRegionForStore(long storeId) {
      List<TiRegion> regionToRemove = new ArrayList<>();
      for (TiRegion r : regionCache.values()) {
        if (r.getLeader().getStoreId() == storeId) {
          if (logger.isDebugEnabled()) {
            logger.debug(String.format("invalidateAllRegionForStore Region[%s]", r));
          }
          regionToRemove.add(r);
        }
      }

      // remove region
      for (TiRegion r : regionToRemove) {
        regionCache.remove(r.getId());
        keyToRegionIdCache.remove(makeRange(r.getStartKey(), r.getEndKey()));
      }
    }

    public synchronized void invalidateStore(long storeId) {
      storeCache.remove(storeId);
    }

    public synchronized Store getStoreById(long id) {
      try {
        Store store = storeCache.get(id);
        if (store == null) {
          store = pdClient.getStore(ConcreteBackOffer.newGetBackOff(), id);
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

  public TiSession getSession() {
    return pdClient.getSession();
  }

  public TiRegion getRegionByKey(ByteString key) {
    return cache.getRegionByKey(key);
  }

  public TiRegion getRegionByRawKey(ByteString key) {
    return cache.getRegionByRawKey(key);
  }

  public TiRegion getRegionById(long regionId) {
    return cache.getRegionById(regionId);
  }

  public Pair<TiRegion, Store> getRegionStorePairByKey(ByteString key) {
    TiRegion region = cache.getRegionByKey(key);
    if (region == null) {
      throw new TiClientInternalException("Region not exist for key:" + formatBytes(key));
    }
    if (!region.isValid()) {
      throw new TiClientInternalException("Region invalid: " + region.toString());
    }
    Peer leader = region.getLeader();
    long storeId = leader.getStoreId();
    return Pair.create(region, cache.getStoreById(storeId));
  }

  public Pair<TiRegion, Store> getRegionStorePairByRawKey(ByteString key) {
    TiRegion region = cache.getRegionByRawKey(key);
    if (region == null) {
      throw new TiClientInternalException("Region not exist for key:" + formatBytes(key));
    }
    if (!region.isValid()) {
      throw new TiClientInternalException("Region invalid: " + region.toString());
    }
    Peer leader = region.getLeader();
    long storeId = leader.getStoreId();
    return Pair.create(region, cache.getStoreById(storeId));
  }

  public Pair<TiRegion, Store> getRegionStorePairByRegionId(long id) {
    TiRegion region = cache.getRegionById(id);
    if (!region.isValid()) {
      throw new TiClientInternalException("Region invalid: " + region.toString());
    }
    Peer leader = region.getLeader();
    long storeId = leader.getStoreId();
    return Pair.create(region, cache.getStoreById(storeId));
  }

  public Store getStoreById(long id) {
    return cache.getStoreById(id);
  }

  public void onRegionStale(long regionId) {
    cache.invalidateRegion(regionId);
  }

  public boolean updateLeader(long regionId, long storeId) {
    TiRegion r = cache.regionCache.get(regionId);
    if (r != null) {
      if (!r.switchPeer(storeId)) {
        // failed to switch leader, possibly region is outdated, we need to drop region cache from
        // regionCache
        logger.warn("Cannot find peer when updating leader (" + regionId + "," + storeId + ")");
        // drop region cache using verId
        cache.invalidateRegion(regionId);
        return false;
      }
    }
    return true;
  }

  /**
   * Clears all cache when a TiKV server does not respond
   *
   * @param regionId region's id
   * @param storeId TiKV store's id
   */
  public void onRequestFail(long regionId, long storeId) {
    cache.invalidateRegion(regionId);
    cache.invalidateAllRegionForStore(storeId);
  }

  public void invalidateStore(long storeId) {
    cache.invalidateStore(storeId);
  }

  public void invalidateRegion(long regionId) {
    cache.invalidateRegion(regionId);
  }
}
