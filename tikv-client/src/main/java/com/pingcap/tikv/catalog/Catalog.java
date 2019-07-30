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

package com.pingcap.tikv.catalog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;

public class Catalog implements AutoCloseable {
  private Supplier<Snapshot> snapshotProvider;
  private ScheduledExecutorService service;
  private CatalogCache metaCache;
  private final boolean showRowId;
  private final String dbPrefix;
  private final Logger logger = Logger.getLogger(this.getClass());

  @Override
  public void close() throws Exception {
    if (service != null) {
      service.shutdownNow();
      service.awaitTermination(1, TimeUnit.SECONDS);
    }
  }

  private static class CatalogCache {

    private CatalogCache(CatalogTransaction transaction, String dbPrefix, boolean loadTables) {
      this.transaction = transaction;
      this.dbPrefix = dbPrefix;
      this.tableCache = new ConcurrentHashMap<>();
      this.dbCache = loadDatabases(loadTables);
      this.currentVersion = transaction.getLatestSchemaVersion();
    }

    private final Map<String, TiDBInfo> dbCache;
    private final ConcurrentHashMap<TiDBInfo, Map<String, TiTableInfo>> tableCache;
    private CatalogTransaction transaction;
    private long currentVersion;
    private final String dbPrefix;

    public CatalogTransaction getTransaction() {
      return transaction;
    }

    public long getVersion() {
      return currentVersion;
    }

    public TiDBInfo getDatabase(String name) {
      Objects.requireNonNull(name, "name is null");
      return dbCache.get(name.toLowerCase());
    }

    public List<TiDBInfo> listDatabases() {
      return ImmutableList.copyOf(dbCache.values());
    }

    public List<TiTableInfo> listTables(TiDBInfo db) {
      Map<String, TiTableInfo> tableMap = tableCache.get(db);
      if (tableMap == null) {
        tableMap = loadTables(db);
      }
      Collection<TiTableInfo> tables = tableMap.values();
      tables.removeIf(TiTableInfo::isView);
      return ImmutableList.copyOf(tables);
    }

    public TiTableInfo getTable(TiDBInfo db, String tableName) {
      Map<String, TiTableInfo> tableMap = tableCache.get(db);
      if (tableMap == null) {
        tableMap = loadTables(db);
      }
      TiTableInfo tbl = tableMap.get(tableName.toLowerCase());
      //https://github.com/pingcap/tispark/issues/961
      // TODO: support reading from view table in the future.
      if (tbl!=null&&tbl.isView()) return null;
      return tbl;
    }

    private Map<String, TiTableInfo> loadTables(TiDBInfo db) {
      List<TiTableInfo> tables = transaction.getTables(db.getId());
      ImmutableMap.Builder<String, TiTableInfo> builder = ImmutableMap.builder();
      for (TiTableInfo table : tables) {
        builder.put(table.getName().toLowerCase(), table);
      }
      Map<String, TiTableInfo> tableMap = builder.build();
      tableCache.put(db, tableMap);
      return tableMap;
    }

    private Map<String, TiDBInfo> loadDatabases(boolean loadTables) {
      HashMap<String, TiDBInfo> newDBCache = new HashMap<>();

      List<TiDBInfo> databases = transaction.getDatabases();
      databases.forEach(
          db -> {
            TiDBInfo newDBInfo = db.rename(dbPrefix + db.getName());
            newDBCache.put(newDBInfo.getName().toLowerCase(), newDBInfo);
            if (loadTables) {
              loadTables(newDBInfo);
            }
          });
      return newDBCache;
    }
  }

  public Catalog(
      Supplier<Snapshot> snapshotProvider,
      int refreshPeriod,
      TimeUnit periodUnit,
      boolean showRowId,
      String dbPrefix) {
    this.snapshotProvider = Objects.requireNonNull(snapshotProvider, "Snapshot Provider is null");
    this.showRowId = showRowId;
    this.dbPrefix = dbPrefix;
    metaCache = new CatalogCache(new CatalogTransaction(snapshotProvider.get()), dbPrefix, false);
    service =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true).build());
    service.scheduleAtFixedRate(
        () -> {
          // Wrap this with a try catch block in case schedule update fails
          try {
            reloadCache(true);
          } catch (Exception e) {
            logger.warn("Reload Cache failed", e);
          }
        },
        refreshPeriod,
        refreshPeriod,
        periodUnit);
  }

  /**
   * read current row id from TiKV and write the calculated value back to TiKV. The calculation rule
   * is start(read from TiKV) + step.
   */
  public synchronized long getAutoTableId(long dbId, long tableId, long step) {
    Snapshot snapshot = snapshotProvider.get();
    CatalogTransaction newTrx = new CatalogTransaction(snapshot);
    return newTrx.getAutoTableId(dbId, tableId, step);
  }

  /** read current row id from TiKV according to database id and table id. */
  public synchronized long getAutoTableId(long dbId, long tableId) {
    Snapshot snapshot = snapshotProvider.get();
    CatalogTransaction newTrx = new CatalogTransaction(snapshot);
    return newTrx.getAutoTableId(dbId, tableId);
  }

  public synchronized void reloadCache(boolean loadTables) {
    Snapshot snapshot = snapshotProvider.get();
    CatalogTransaction newTrx = new CatalogTransaction(snapshot);
    long latestVersion = newTrx.getLatestSchemaVersion();
    if (latestVersion > metaCache.getVersion()) {
      metaCache = new CatalogCache(newTrx, dbPrefix, loadTables);
    }
  }

  public void reloadCache() {
    reloadCache(false);
  }

  public List<TiDBInfo> listDatabases() {
    return metaCache.listDatabases();
  }

  public List<TiTableInfo> listTables(TiDBInfo database) {
    Objects.requireNonNull(database, "database is null");
    if (showRowId) {
      return metaCache
          .listTables(database)
          .stream()
          .map(TiTableInfo::copyTableWithRowId)
          .collect(Collectors.toList());
    } else {
      return metaCache.listTables(database);
    }
  }

  public TiDBInfo getDatabase(String dbName) {
    Objects.requireNonNull(dbName, "dbName is null");
    TiDBInfo dbInfo = metaCache.getDatabase(dbName);
    if (dbInfo == null) {
      // reload cache if database does not exist
      reloadCache(true);
      dbInfo = metaCache.getDatabase(dbName);
    }
    return dbInfo;
  }

  public TiTableInfo getTable(String dbName, String tableName) {
    TiDBInfo database = getDatabase(dbName);
    if (database == null) {
      return null;
    }
    return getTable(database, tableName);
  }

  public TiTableInfo getTable(TiDBInfo database, String tableName) {
    Objects.requireNonNull(database, "database is null");
    Objects.requireNonNull(tableName, "tableName is null");
    TiTableInfo table = metaCache.getTable(database, tableName);
    if (table == null) {
      // reload cache if table does not exist
      reloadCache(true);
      table = metaCache.getTable(database, tableName);
    }
    if (showRowId && table != null) {
      return table.copyTableWithRowId();
    } else {
      return table;
    }
  }

  @VisibleForTesting
  public TiTableInfo getTable(TiDBInfo database, long tableId) {
    Objects.requireNonNull(database, "database is null");
    Collection<TiTableInfo> tables = listTables(database);
    for (TiTableInfo table : tables) {
      if (table.getId() == tableId) {
        if (showRowId) {
          return table.copyTableWithRowId();
        } else {
          return table;
        }
      }
    }
    return null;
  }
}
