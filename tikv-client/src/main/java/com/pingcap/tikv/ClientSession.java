package com.pingcap.tikv;

import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.util.ConverterUpstream;
import java.util.function.Function;
import lombok.Getter;
import org.tikv.common.event.CacheInvalidateEvent;
import org.tikv.common.meta.TiTimestamp;

@Getter
public class ClientSession implements AutoCloseable {
  private final TiConfiguration conf;
  private final org.tikv.common.TiSession tikvSession;
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
    this.conf = config;
    this.tikvSession =
        org.tikv.common.TiSession.create(ConverterUpstream.convertTiConfiguration(config));
    this.catalog = new Catalog(this::createSnapshot, config.isShowRowId(), config.getDBPrefix());
  }

  public Snapshot createSnapshot() {
    // checkIsClosed();
    return new Snapshot(this.tikvSession.getTimestamp(), this.conf);
  }

  public Snapshot createSnapshot(TiTimestamp ts) {
    //     checkIsClosed();
    return new Snapshot(ts, this.conf);
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
    return new Snapshot(snapshotTimestamp, conf);
  }

  private volatile TiTimestamp snapshotTimestamp;
  private volatile Catalog snapshotCatalog;

  public static ClientSession getInstance(com.pingcap.tikv.TiConfiguration config) {
    return new ClientSession(config);
  }

  @Override
  public void close() throws Exception {
    tikvSession.close();
  }
}
