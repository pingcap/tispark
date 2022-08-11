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

import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.util.ConverterUpstream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import lombok.Getter;
import org.tikv.common.TiSession;
import org.tikv.common.event.CacheInvalidateEvent;
import org.tikv.common.meta.TiTimestamp;

@Getter
public class ClientSession implements AutoCloseable {

  private static final Map<String, ClientSession> sessionCachedMap = new HashMap<>();
  private final TiConfiguration conf;
  private final TiSession tikvSession;
  private final Catalog catalog;
  private Function<CacheInvalidateEvent, Void> cacheInvalidateCallback;

  /**
   * This is used for setting call back function to invalidate cache information
   *
   * @param callBackFunc callback function
   */
  public void injectCallBackFunc(Function<CacheInvalidateEvent, Void> callBackFunc) {
    this.cacheInvalidateCallback = callBackFunc;
  }

  private ClientSession(com.pingcap.tikv.TiConfiguration config) {
    if (config != null) {
      this.conf = config;
    } else {
      this.conf = com.pingcap.tikv.TiConfiguration.createDefault("127.0.0.1:2379");
    }
    this.tikvSession =
        org.tikv.common.TiSession.getInstance(ConverterUpstream.convertTiConfiguration(getConf()));
    this.catalog =
        new Catalog(this::createSnapshot, getConf().isShowRowId(), getConf().getDBPrefix());
  }

  public Snapshot createSnapshot() {
    // checkIsClosed();
    return new Snapshot(this.tikvSession.getTimestamp(), this);
  }

  public Snapshot createSnapshot(TiTimestamp ts) {
    //     checkIsClosed();
    return new Snapshot(ts, this);
  }

  public synchronized Catalog getOrCreateSnapShotCatalog(TiTimestamp ts) {
    snapshotTimestamp = ts;
    if (snapshotCatalog == null) {
      snapshotCatalog =
          new Catalog(
              this::createSnapshotWithSnapshotTimestamp, conf.isShowRowId(), conf.getDBPrefix());
    }
    snapshotCatalog.reloadCache(true);
    return snapshotCatalog;
  }

  public Snapshot createSnapshotWithSnapshotTimestamp() {
    return new Snapshot(snapshotTimestamp, this);
  }

  private volatile TiTimestamp snapshotTimestamp;
  private volatile Catalog snapshotCatalog;

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

  @Override
  public void close() throws Exception {
    tikvSession.close();
  }
}
