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
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Catalog implements AutoCloseable {
  private final boolean showRowId;
  private final String dbPrefix;
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final Supplier<Snapshot> snapshotProvider;
  private CatalogCache metaCache;

  public Catalog(Supplier<Snapshot> snapshotProvider, boolean showRowId, String dbPrefix) {
    this.snapshotProvider = Objects.requireNonNull(snapshotProvider, "Snapshot Provider is null");
    this.showRowId = showRowId;
    this.dbPrefix = dbPrefix;
    metaCache = new CatalogCache(new CatalogTransaction(snapshotProvider.get()), dbPrefix, false);
  }

  @Override
  public void close() {}

  private synchronized void reloadCache(boolean loadTables) {
    Snapshot snapshot = snapshotProvider.get();
    CatalogTransaction newTrx = new CatalogTransaction(snapshot);
    long latestVersion = newTrx.getLatestSchemaVersion();
    if (latestVersion > metaCache.getVersion()) {
      metaCache = new CatalogCache(newTrx, dbPrefix, loadTables);
    }
  }

  private void reloadCache() {
    reloadCache(false);
  }

  public List<TiDBInfo> listDatabases() {
    reloadCache();
    return metaCache.listDatabases();
  }

  public List<TiTableInfo> listTables(TiDBInfo database) {
    Objects.requireNonNull(database, "database is null");
    reloadCache(true);
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
    reloadCache();
    return metaCache.getDatabase(dbName);
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
    reloadCache(true);
    TiTableInfo table = metaCache.getTable(database, tableName);
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

  private static class CatalogCache {

    private final Map<String, TiDBInfo> dbCache;
    private final ConcurrentHashMap<TiDBInfo, Map<String, TiTableInfo>> tableCache;
    private final String dbPrefix;
    private final CatalogTransaction transaction;
    private final long currentVersion;

    private CatalogCache(CatalogTransaction transaction, String dbPrefix, boolean loadTables) {
      this.transaction = transaction;
      this.dbPrefix = dbPrefix;
      this.tableCache = new ConcurrentHashMap<>();
      this.dbCache = loadDatabases(loadTables);
      this.currentVersion = transaction.getLatestSchemaVersion();
    }

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
      return tables
          .stream()
          .filter(tbl -> !tbl.isView() || !tbl.isSequence())
          .collect(Collectors.toList());
    }

    public TiTableInfo getTable(TiDBInfo db, String tableName) {
      Map<String, TiTableInfo> tableMap = tableCache.get(db);
      if (tableMap == null) {
        tableMap = loadTables(db);
      }
      TiTableInfo tbl = tableMap.get(tableName.toLowerCase());
      // https://github.com/pingcap/tispark/issues/961
      // TODO: support reading from view table in the future.
      if (tbl != null && (tbl.isView() || tbl.isSequence())) return null;
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
}
