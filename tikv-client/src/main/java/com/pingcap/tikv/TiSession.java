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

package com.pingcap.tikv;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.event.CacheInvalidateEvent;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.key.RowKey;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.txn.TxnKVClient;
import com.pingcap.tikv.util.ChannelFactory;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;
import gnu.trove.list.array.TLongArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Metapb;

public class TiSession implements AutoCloseable {
  private final Logger logger = LoggerFactory.getLogger(TiSession.class);
  private final TiConfiguration conf;
  private final ChannelFactory channelFactory;
  private Function<CacheInvalidateEvent, Void> cacheInvalidateCallback;
  // below object creation is either heavy or making connection (pd), pending for lazy loading
  private volatile PDClient client;
  private volatile Catalog catalog;
  private volatile ExecutorService indexScanThreadPool;
  private volatile ExecutorService tableScanThreadPool;

  private volatile RegionManager regionManager;
  private volatile RegionStoreClient.RegionStoreClientBuilder clientBuilder;

  public TiSession(TiConfiguration conf) {
    this.conf = conf;
    this.channelFactory = new ChannelFactory(conf.getMaxFrameSize());
    this.regionManager = null;
    this.clientBuilder = null;
  }

  public TxnKVClient createTxnClient() {
    return new TxnKVClient(conf, this.getRegionStoreClientBuilder(), this.getPDClient());
  }

  public RegionStoreClient.RegionStoreClientBuilder getRegionStoreClientBuilder() {
    RegionStoreClient.RegionStoreClientBuilder res = clientBuilder;
    if (res == null) {
      synchronized (this) {
        if (clientBuilder == null) {
          clientBuilder =
              new RegionStoreClient.RegionStoreClientBuilder(
                  conf, this.channelFactory, this.getRegionManager());
        }
        res = clientBuilder;
      }
    }
    return res;
  }

  public TiConfiguration getConf() {
    return conf;
  }

  public TiTimestamp getTimestamp() {
    return getPDClient().getTimestamp(ConcreteBackOffer.newTsoBackOff());
  }

  public Snapshot createSnapshot() {
    return new Snapshot(getTimestamp(), this);
  }

  public Snapshot createSnapshot(TiTimestamp ts) {
    return new Snapshot(ts, this);
  }

  public PDClient getPDClient() {
    PDClient res = client;
    if (res == null) {
      synchronized (this) {
        if (client == null) {
          client = PDClient.createRaw(this.getConf(), channelFactory);
        }
        res = client;
      }
    }
    return res;
  }

  public Catalog getCatalog() {
    Catalog res = catalog;
    if (res == null) {
      synchronized (this) {
        if (catalog == null) {
          catalog =
              new Catalog(
                  this::createSnapshot,
                  conf.getMetaReloadPeriod(),
                  conf.getMetaReloadPeriodUnit(),
                  conf.ifShowRowId(),
                  conf.getDBPrefix());
        }
        res = catalog;
      }
    }
    return res;
  }

  public synchronized RegionManager getRegionManager() {
    RegionManager res = regionManager;
    if (res == null) {
      synchronized (this) {
        if (regionManager == null) {
          regionManager = new RegionManager(getPDClient(), this.cacheInvalidateCallback);
        }
        res = regionManager;
      }
    }
    return res;
  }

  public ExecutorService getThreadPoolForIndexScan() {
    ExecutorService res = indexScanThreadPool;
    if (res == null) {
      synchronized (this) {
        if (indexScanThreadPool == null) {
          indexScanThreadPool =
              Executors.newFixedThreadPool(
                  conf.getIndexScanConcurrency(),
                  new ThreadFactoryBuilder().setDaemon(true).build());
        }
        res = indexScanThreadPool;
      }
    }
    return res;
  }

  public ExecutorService getThreadPoolForTableScan() {
    ExecutorService res = tableScanThreadPool;
    if (res == null) {
      synchronized (this) {
        if (tableScanThreadPool == null) {
          tableScanThreadPool =
              Executors.newFixedThreadPool(
                  conf.getTableScanConcurrency(),
                  new ThreadFactoryBuilder().setDaemon(true).build());
        }
        res = tableScanThreadPool;
      }
    }
    return res;
  }

  public static TiSession create(TiConfiguration conf) {
    return new TiSession(conf);
  }

  /**
   * This is used for setting call back function to invalidate cache information
   *
   * @param callBackFunc callback function
   */
  public void injectCallBackFunc(Function<CacheInvalidateEvent, Void> callBackFunc) {
    this.cacheInvalidateCallback = callBackFunc;
  }

  public void splitRegionAndScatter(List<byte[]> splitKeys) {
    logger.info(String.format("split key's size is %d", splitKeys.size()));
    List<Key> rawKeys =
        splitKeys.parallelStream().map(RowKey::toRawKey).collect(Collectors.toList());
    TLongArrayList regionIds = new TLongArrayList();
    rawKeys.forEach(key -> splitRegionAndScatter(key, regionIds));
    for (int i = 0; i < regionIds.size(); i++) {
      getPDClient().waitScatterRegionFinish(regionIds.get(i));
    }
  }

  // splitKey is a row key, but we use a generalized key here.
  private void splitRegionAndScatter(Key splitKey, TLongArrayList regionIds) {
    // make sure split key must be row key
    Key nextKey = splitKey.next();
    Pair<TiRegion, Metapb.Store> pair =
        getRegionManager().getRegionStorePairByKey(splitKey.toByteString());
    TiRegion region = pair.first;
    Metapb.Store store = pair.second;

    if (nextKey.toByteString().equals(region.getStartKey())
        || nextKey.toByteString().equals(region.getEndKey())) {
      logger.warn(
          "split key equal to region start key or end key. Region splitting " + "is not needed.");
      return;
    }
    TiRegion left =
        getRegionStoreClientBuilder()
            .build(region, store)
            .splitRegion(ByteString.copyFrom(nextKey.getBytes()));
    Objects.requireNonNull(left, "Region after split cannot be null");
    // need invalidate region that is already split.
    getPDClient().scatterRegion(left);
    regionIds.add(left.getId());
    // after split succeed, we need invalidate outdated region info from cache.
    getRegionManager().invalidateRegion(region.getId());
  }

  @Override
  public void close() throws Exception {
    getThreadPoolForTableScan().shutdownNow();
    getThreadPoolForIndexScan().shutdownNow();
    if (client != null) {
      getPDClient().close();
    }
    if (catalog != null) {
      getCatalog().close();
    }
  }
}
