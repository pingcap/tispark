/*
 * Copyright 2022 PingCAP, Inc.
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
 */

package com.pingcap.tikv;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.meta.Collation;
import com.pingcap.tikv.util.ConvertUpstreamUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.Getter;
import org.tikv.common.TiSession;
import org.tikv.common.event.CacheInvalidateEvent;
import org.tikv.common.meta.TiTimestamp;

@Getter
public class ClientSession implements AutoCloseable {

  private static final Map<String, ClientSession> sessionCachedMap = new HashMap<>();
  private final TiConfiguration conf;
  private final TiSession tiKVSession;
  private volatile Catalog catalog;
  private Function<CacheInvalidateEvent, Void> cacheInvalidateCallback;
  private volatile boolean isClosed = false;
  private volatile TiTimestamp snapshotTimestamp;
  private volatile Catalog snapshotCatalog;
  // storeStatusCache will be init at @see DAGIterator#isMppStoreAlive
  private volatile Map<String, Boolean> storeStatusCache;
  private ScheduledExecutorService storeStatusCacheExecutor;

  /**
   * This is used for setting call back function to invalidate cache information
   *
   * @param callBackFunc callback function
   */
  public void injectCallBackFunc(Function<CacheInvalidateEvent, Void> callBackFunc) {
    checkIsClosed();
    this.cacheInvalidateCallback = callBackFunc;
  }

  public Catalog getCatalog() {
    Catalog res = catalog;
    if (res == null) {
      synchronized (this) {
        if (catalog == null) {
          catalog =
              new Catalog(
                  this::createSnapshot,
                  getConf().isShowRowId(),
                  getConf().getDBPrefix(),
                  getConf().getLoadTables());
        }
        res = catalog;
      }
    }
    return res;
  }

  private ClientSession(com.pingcap.tikv.TiConfiguration config) {
    if (config != null) {
      this.conf = config;
    } else {
      this.conf = com.pingcap.tikv.TiConfiguration.createDefault("127.0.0.1:2379");
    }
    this.tiKVSession =
        org.tikv.common.TiSession.create(ConvertUpstreamUtils.convertTiConfiguration(getConf()));
    refreshNewCollationEnabled();
  }

  // if NewCollationEnabled is not set in configuration file,
  // we will set it to true when TiDB version is greater than or equal to v6.0.0.
  // Otherwise, we will set it to false
  private void refreshNewCollationEnabled() {
    if (!conf.getNewCollationEnable().isPresent()) {
      if (ConvertUpstreamUtils.isTiKVVersionGreatEqualThanVersion(
          getTiKVSession().getPDClient(), "6.0.0")) {
        Collation.setNewCollationEnabled(true);
      } else {
        Collation.setNewCollationEnabled(false);
      }
    }
  }

  private void checkIsClosed() {
    if (isClosed) {
      throw new RuntimeException("this TiSession is closed!");
    }
  }

  public Snapshot createSnapshot() {
    checkIsClosed();
    return new Snapshot(this.tiKVSession.getTimestamp(), this);
  }

  public Snapshot createSnapshot(TiTimestamp ts) {
    checkIsClosed();
    return new Snapshot(ts, this);
  }

  public synchronized Catalog getOrCreateSnapShotCatalog(TiTimestamp ts) {
    checkIsClosed();
    snapshotTimestamp = ts;
    if (snapshotCatalog == null) {
      snapshotCatalog =
          new Catalog(
              this::createSnapshotWithSnapshotTimestamp,
              conf.isShowRowId(),
              conf.getDBPrefix(),
              conf.getLoadTables());
    }
    return snapshotCatalog;
  }

  public Snapshot createSnapshotWithSnapshotTimestamp() {
    checkIsClosed();
    return new Snapshot(snapshotTimestamp, this);
  }

  public static ClientSession getInstance(com.pingcap.tikv.TiConfiguration config) {
    synchronized (sessionCachedMap) {
      String key = config.getPdAddrsString();
      if (sessionCachedMap.containsKey(key)) {
        return sessionCachedMap.get(key);
      }
      ClientSession newSession = new ClientSession(config);
      sessionCachedMap.put(key, newSession);
      return newSession;
    }
  }

  public Map<String, Boolean> getStoreStatusCache() {
    if (storeStatusCache == null) {
      synchronized (this) {
        if (storeStatusCache == null) {
          storeStatusCache = new ConcurrentHashMap<>();
          storeStatusCacheExecutor =
              Executors.newSingleThreadScheduledExecutor(
                  new ThreadFactoryBuilder()
                      .setNameFormat("storeStatus-thread-%d")
                      .setDaemon(true)
                      .build());
          storeStatusCacheExecutor.scheduleAtFixedRate(
              () -> {
                storeStatusCache.replaceAll(
                    (k, v) ->
                        TiFlashClient.isMppAlive(
                            this.tiKVSession
                                .getChannelFactory()
                                .getChannel(k, this.tiKVSession.getPDClient().getHostMapping())));
              },
              0,
              5,
              TimeUnit.SECONDS);
        }
      }
    }
    return storeStatusCache;
  }

  @Override
  public void close() throws Exception {
    shutdown();
  }

  private synchronized void shutdown() throws Exception {
    if (tiKVSession != null) {
      tiKVSession.close();
    }
    if (!isClosed) {
      isClosed = true;
      if (snapshotCatalog != null) {
        snapshotCatalog.close();
      }
      if (storeStatusCacheExecutor != null) {
        storeStatusCacheExecutor.shutdownNow();
      }
      synchronized (sessionCachedMap) {
        sessionCachedMap.remove(conf.getPdAddrsString());
      }
    }
  }
}
