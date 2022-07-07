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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
import com.pingcap.tikv.ReadOnlyPDClient;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.event.CacheInvalidateEvent;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.ConverterUpstream;
import com.pingcap.tikv.util.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.exception.InvalidStoreException;
import org.tikv.common.region.StoreHealthyChecker;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.region.TiStoreType;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Peer;
import org.tikv.kvproto.Metapb.StoreState;
import org.tikv.shade.com.google.protobuf.ByteString;

@SuppressWarnings("UnstableApiUsage")
public class RegionManager {
  private static final Logger logger = LoggerFactory.getLogger(RegionManager.class);
  // TODO: the region cache logic need rewrite.
  // https://github.com/pingcap/tispark/issues/1170
  private final RegionCache cache;
  private final ReadOnlyPDClient pdClient;
  private final TiConfiguration conf;
  private final StoreHealthyChecker storeChecker;

  private final Function<CacheInvalidateEvent, Void> cacheInvalidateCallback;

  // To avoid double retrieval, we used the async version of grpc
  // When rpc not returned, instead of call again, it wait for previous one done
  public RegionManager(
      TiConfiguration conf,
      ReadOnlyPDClient pdClient,
      Function<CacheInvalidateEvent, Void> cacheInvalidateCallback) {
    this.cache = new RegionCache(pdClient);
    this.cacheInvalidateCallback = cacheInvalidateCallback;
    this.pdClient = pdClient;
    this.conf = conf;
    this.storeChecker = null;
  }

  public RegionManager(TiConfiguration conf, ReadOnlyPDClient pdClient) {
    this.cache = new RegionCache(pdClient);
    this.cacheInvalidateCallback = null;
    this.pdClient = pdClient;
    this.conf = conf;
    this.storeChecker = null;
  }

  public Function<CacheInvalidateEvent, Void> getCacheInvalidateCallback() {
    return cacheInvalidateCallback;
  }

  public ReadOnlyPDClient getPDClient() {
    return this.cache.pdClient;
  }

  public TiRegion getRegionByKey(ByteString key) {
    return getRegionByKey(key, ConcreteBackOffer.newGetBackOff(getClusterId()));
  }

  public TiRegion getRegionByKey(ByteString key, BackOffer backOffer) {
    TiRegion region = cache.getRegionByKey(key, backOffer);
    try {
      if (region == null) {
        logger.debug("Key not found in keyToRegionIdCache:" + formatBytesUTF8(key));
        Pair<Metapb.Region, Metapb.Peer> regionAndLeader = pdClient.getRegionByKey(backOffer, key);
        region =
            cache.putRegion(createRegion(regionAndLeader.first, regionAndLeader.second, backOffer));
      }
    } catch (Exception e) {
      return null;
    }
    return region;
  }

  public Pair<TiRegion, TiStore> getRegionStorePairByKey(ByteString key, BackOffer backOffer) {
    return getRegionStorePairByKey(key, TiStoreType.TiKV, backOffer);
  }

  public Pair<TiRegion, TiStore> getRegionStorePairByKey(ByteString key) {
    return getRegionStorePairByKey(key, TiStoreType.TiKV);
  }

  public Pair<TiRegion, TiStore> getRegionStorePairByKey(ByteString key, TiStoreType storeType) {
    return getRegionStorePairByKey(key, storeType, ConcreteBackOffer.newGetBackOff(getClusterId()));
  }

  public long getClusterId() {
    // Be careful to pass null as PDClient in the constructor
    return getPDClient() != null ? getPDClient().getClusterId() : 0L;
  }

  public Pair<TiRegion, TiStore> getRegionStorePairByKey(
      ByteString key, TiStoreType storeType, BackOffer backOffer) {
    TiRegion region = getRegionByKey(key, backOffer);
    if (region == null) {
      throw new TiClientInternalException("Region not exist for key:" + formatBytesUTF8(key));
    }
    if (!region.isValid()) {
      throw new TiClientInternalException("Region invalid: " + region.toString());
    }

    TiStore store = null;
    if (storeType == TiStoreType.TiKV) {
      Peer leader = region.getLeader();
      store = getStoreById(leader.getStoreId(), backOffer);
    } else {
      outerLoop:
      for (Peer peer : region.getLearnerList()) {
        TiStore s = getStoreById(peer.getStoreId(), backOffer);
        for (Metapb.StoreLabel label : s.getStore().getLabelsList()) {
          if (label.getKey().equals(storeType.getLabelKey())
              && label.getValue().equals(storeType.getLabelValue())) {
            store = s;
            break outerLoop;
          }
        }
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

  public TiStore getStoreById(long id) {
    return getStoreById(id, ConcreteBackOffer.newGetBackOff(getClusterId()));
  }

  private TiStore getStoreByIdWithBackOff(long id, BackOffer backOffer) {
    try {
      TiStore store = cache.getStoreById(id, backOffer);
      if (store == null) {
        store = new TiStore(pdClient.getStore(backOffer, id));
      } else {
        return store;
      }
      // if we did not get store info from pd, remove store from cache
      if (store.getStore() == null) {
        logger.warn(String.format("failed to get store %d from pd", id));
        return null;
      }
      // if the store is already tombstone, remove store from cache
      if (store.getStore().getState().equals(StoreState.Tombstone)) {
        logger.warn(String.format("store %d is tombstone", id));
        return null;
      }
      if (cache.putStore(id, store) && storeChecker != null) {
        storeChecker.scheduleStoreHealthCheck(store);
      }
      return store;
    } catch (Exception e) {
      throw new GrpcException(e);
    }
  }

  public TiStore getStoreById(long id, BackOffer backOffer) {
    //    return cache.getStoreById(id, backOffer);
    TiStore store = getStoreByIdWithBackOff(id, backOffer);
    if (store == null) {
      logger.warn(String.format("failed to fetch store %d, the store may be missing", id));
      throw new InvalidStoreException(id);
    }
    return store;
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

  private TiRegion createRegion(Metapb.Region region, Metapb.Peer leader, BackOffer backOffer) {
    List<Metapb.Peer> peers = new ArrayList<>();
    List<TiStore> stores = new ArrayList<>();
    for (Metapb.Peer peer : region.getPeersList()) {
      try {
        stores.add(getStoreById(peer.getStoreId(), backOffer));
        peers.add(peer);
      } catch (Exception e) {
        logger.warn("Store {} not found: {}", peer.getStoreId(), e.toString());
      }
    }
    Metapb.Region newRegion =
        Metapb.Region.newBuilder().mergeFrom(region).clearPeers().addAllPeers(peers).build();
    return new TiRegion(
        ConverterUpstream.convertTiConfiguration(conf), newRegion, leader, peers, stores);
  }

  public static class RegionCache {
    // private final Map<Long, TiRegion> regionCache;
    private final Map<Long, TiStore> storeCache;
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
      return region;
    }

    private synchronized TiRegion putRegion(TiRegion region) {
      if (logger.isDebugEnabled()) {
        logger.debug("putRegion: " + region);
      }
      regionCache.remove(makeRange(region.getStartKey(), region.getEndKey()));
      regionCache.put(makeRange(region.getStartKey(), region.getEndKey()), region);
      return region;
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
    }

    /** Removes region associated with regionId from regionCache. */
    public synchronized void invalidateRegion(TiRegion region) {
      try {
        if (logger.isDebugEnabled()) {
          logger.debug(String.format("invalidateRegion ID[%s]", region.getId()));
        }
        TiRegion oldRegion = regionCache.get(getEncodedKey(region.getStartKey()));
        if (oldRegion != null && oldRegion.equals(region)) {
          regionCache.remove(makeRange(region.getStartKey(), region.getEndKey()));
        }
      } catch (Exception ignore) {
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

    public synchronized boolean putStore(long id, TiStore store) {
      TiStore oldStore = storeCache.get(id);
      if (oldStore != null) {
        if (oldStore.equals(store)) {
          return false;
        } else {
          oldStore.markInvalid();
        }
      }
      storeCache.put(id, store);
      return true;
    }

    public synchronized TiStore getStoreById(long id, BackOffer backOffer) {
      try {
        TiStore store = storeCache.get(id);
        if (store == null) {
          store = new TiStore(pdClient.getStore(backOffer, id));
        }
        if (store.getStore().getState().equals(StoreState.Tombstone)) {
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
