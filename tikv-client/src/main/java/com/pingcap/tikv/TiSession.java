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
import com.pingcap.tikv.codec.Codec.BytesCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.event.CacheInvalidateEvent;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.txn.TxnKVClient;
import com.pingcap.tikv.util.ChannelFactory;
import com.pingcap.tikv.util.ConcreteBackOffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class TiSession implements AutoCloseable {
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

  public void splitRegionAndScatter(List<ByteString> splitKeys) {
    splitKeys.forEach(this::splitRegionAndScatter);
  }

  public void splitRegionAndScatter(ByteString encodeKey) {
    TiRegion region = regionManager.getRegionByKey(encodeKey);
    // this need key which is not encoded.

    ByteString decodedKey =
        ByteString.copyFrom(BytesCodec.readBytes(new CodecDataInput(encodeKey.toByteArray())));
    TiRegion left = getRegionStoreClientBuilder().build(region).splitRegion(decodedKey);
    getPDClient().scatterRegion(left);
  }

  @Override
  public void close() throws Exception {
    getThreadPoolForTableScan().shutdownNow();
    getThreadPoolForIndexScan().shutdownNow();
    getPDClient().close();
  }
}
