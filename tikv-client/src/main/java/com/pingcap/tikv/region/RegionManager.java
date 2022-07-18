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
import static org.tikv.common.region.RegionManager.GET_REGION_BY_KEY_REQUEST_LATENCY;

import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.event.CacheInvalidateEvent;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.ConverterUpstream;
import com.pingcap.tikv.util.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.ReadOnlyPDClient;
import org.tikv.common.codec.KeyUtils;
import org.tikv.common.exception.InvalidStoreException;
import org.tikv.common.log.SlowLogSpan;
import org.tikv.common.region.RegionCache;
import org.tikv.common.region.StoreHealthyChecker;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.region.TiStoreType;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Peer;
import org.tikv.kvproto.Metapb.Region;
import org.tikv.kvproto.Metapb.StoreState;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.shade.io.prometheus.client.Histogram;

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
    this.cache = new RegionCache();
    this.cacheInvalidateCallback = cacheInvalidateCallback;
    this.pdClient = pdClient;
    this.conf = conf;
    this.storeChecker = null;
  }

  public RegionManager(TiConfiguration conf, ReadOnlyPDClient pdClient) {
    this.cache = new RegionCache();
    this.cacheInvalidateCallback = null;
    this.pdClient = pdClient;
    this.conf = conf;
    this.storeChecker = null;
  }

  public Function<CacheInvalidateEvent, Void> getCacheInvalidateCallback() {
    return cacheInvalidateCallback;
  }

  public ReadOnlyPDClient getPDClient() {
    return this.pdClient;
  }

  public TiRegion getRegionByKey(ByteString key) {
    return getRegionByKey(
        key, ConcreteBackOffer.newCustomBackOff(BackOffer.RAWKV_MAX_BACKOFF, getClusterId()));
  }

  public TiRegion getRegionByKey(ByteString key, BackOffer backOffer) {
    Long clusterId = pdClient.getClusterId();
    Histogram.Timer requestTimer =
        GET_REGION_BY_KEY_REQUEST_LATENCY.labels(clusterId.toString()).startTimer();
    SlowLogSpan slowLogSpan = backOffer.getSlowLog().start("getRegionByKey");
    TiRegion region = cache.getRegionByKey(key, backOffer);
    try {
      if (region == null) {
        logger.debug("Key not found in keyToRegionIdCache:" + KeyUtils.formatBytesUTF8(key));
        Pair<Region, Peer> regionAndLeader =
            (Pair<Region, Peer>) pdClient.getRegionByKey(backOffer, key);
        region =
            cache.putRegion(createRegion(regionAndLeader.first, regionAndLeader.second, backOffer));
      }
    } catch (Exception e) {
      return null;
    } finally {
      requestTimer.observeDuration();
      slowLogSpan.end();
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
    return getRegionStorePairByKey(
        key,
        storeType,
        ConcreteBackOffer.newCustomBackOff(BackOffer.RAWKV_MAX_BACKOFF, getClusterId()));
  }

  public long getClusterId() {
    // Be careful to pass null as PDClient in the constructor
    // TODO:Careful confirmation/doubt required
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
        cache.invalidateRegion(region);
      }
    }

    if (store == null) {
      throw new TiClientInternalException(
          "Cannot find valid store on " + storeType + " for region " + region.toString());
    }

    return Pair.create(region, store);
  }

  public TiStore getStoreById(long id) {
    return getStoreById(
        id, ConcreteBackOffer.newCustomBackOff(BackOffer.RAWKV_MAX_BACKOFF, getClusterId()));
  }

  private TiStore getStoreByIdWithBackOff(long id, BackOffer backOffer) {
    try {
      TiStore store = cache.getStoreById(id);
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
    TiStore store = getStoreByIdWithBackOff(id, backOffer);
    if (store == null) {
      logger.warn(String.format("failed to fetch store %d, the store may be missing", id));
      cache.clearAll();
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
        // TODO:Careful confirmation/doubt required
        logger.warn("Store {} not found: {}", peer.getStoreId(), e.toString());
      }
    }
    Metapb.Region newRegion =
        Metapb.Region.newBuilder().mergeFrom(region).clearPeers().addAllPeers(peers).build();
    return new TiRegion(
        ConverterUpstream.convertTiConfiguration(conf), newRegion, leader, peers, stores);
  }
}
