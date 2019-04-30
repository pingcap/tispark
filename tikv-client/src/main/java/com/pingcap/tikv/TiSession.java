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
import gnu.trove.list.array.TLongArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiSession implements AutoCloseable {
  private final Logger logger = LoggerFactory.getLogger(TiSession.class);
  private final TiConfiguration conf;
  private final ChannelFactory channelFactory;
  private Function<CacheInvalidateEvent, Void> cacheInvalidateCallback;
  // below object creation is either heavy or making connection (pd), pending for lazy loading
  private volatile RegionManager regionManager;
  private volatile PDClient client;
  private volatile Catalog catalog;
  private volatile ExecutorService indexScanThreadPool;
  private volatile ExecutorService tableScanThreadPool;

  private final RegionStoreClient.RegionStoreClientBuilder clientBuilder;

  public TiSession(TiConfiguration conf) {
    this.conf = conf;
    this.channelFactory = new ChannelFactory(conf.getMaxFrameSize());
    this.regionManager = new RegionManager(this.getPDClient(), this.cacheInvalidateCallback);
    this.clientBuilder =
        new RegionStoreClient.RegionStoreClientBuilder(
            conf, this.channelFactory, this.regionManager, this);
  }

  public TxnKVClient createTxnClient() {
    // Create new Region Manager avoiding thread contentions
    RegionManager regionMgr = new RegionManager(this.getPDClient(), this.cacheInvalidateCallback);
    RegionStoreClient.RegionStoreClientBuilder builder =
        new RegionStoreClient.RegionStoreClientBuilder(conf, this.channelFactory, regionMgr, this);
    return new TxnKVClient(conf, builder, this.getPDClient());
  }

  public RegionStoreClient.RegionStoreClientBuilder getRegionStoreClientBuilder() {
    return this.clientBuilder;
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
    // TODO: adding a conf let user to decide wait or not.
    for(int i = 0; i< regionIds.size(); i++) {
      getPDClient().waitScatterRegionFinish(regionIds.get(i));
    }
  }


  // splitKey is a row key, but we use a generalized key here.
  private void splitRegionAndScatter(Key splitKey, TLongArrayList regionIds) {
    // make sure split key must be row key
    Key nextKey = splitKey.next();
    TiRegion region = regionManager.getRegionByKey(splitKey.toByteString());
    if (nextKey.toByteString().equals(region.getStartKey())
        || nextKey.toByteString().equals(region.getEndKey())) {
      logger.warn(
          "split key equal to region start key or end key. Region splitting " + "is not needed.");
      return;
    }
    TiRegion left =
        getRegionStoreClientBuilder()
            .build(region)
            .splitRegion(ByteString.copyFrom(nextKey.getBytes()));
    Objects.requireNonNull(left, "Region after split cannot be null");
    // need invalidate region that is already split.
    getPDClient().scatterRegion(left);
    regionIds.add(left.getId());
    // after split succeed, we need invalidate outdated region info from cache.
    regionManager.invalidateRegion(region.getId());
  }

  @Override
  public void close() throws Exception {
    getThreadPoolForTableScan().shutdownNow();
    getThreadPoolForIndexScan().shutdownNow();
    getPDClient().close();
  }
}
