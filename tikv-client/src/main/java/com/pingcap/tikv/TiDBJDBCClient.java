/*
 * Copyright 2019 PingCAP, Inc.
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBJDBCClient implements AutoCloseable {
  private static final String UNLOCK_TABLES_SQL = "unlock tables";
  private static final String SELECT_TIDB_CONFIG_SQL = "select @@tidb_config";
  private static final String ENABLE_TABLE_LOCK_KEY = "enable-table-lock";
  private static final Boolean ENABLE_TABLE_LOCK_DEFAULT = false;
  private static final String DELAY_CLEAN_TABLE_LOCK = "delay-clean-table-lock";
  private static final int DELAY_CLEAN_TABLE_LOCK_DEFAULT = 0;
  private static final String TIDB_ROW_FORMAT_VERSION_SQL = "select @@tidb_row_format_version";
  private static final int TIDB_ROW_FORMAT_VERSION_DEFAULT = 1;
  private static final String ALTER_PRIMARY_KEY_KEY = "alter-primary-key";
  private static final Boolean ALTER_PRIMARY_KEY_DEFAULT = false;
  private final Logger logger = LoggerFactory.getLogger(getClass().getName());
  private final Connection connection;

  public TiDBJDBCClient(Connection connection) {
    this.connection = connection;
  }

  public boolean isEnableAlterPrimaryKey() throws IOException, SQLException {
    Map<String, Object> configMap = readConfMapFromTiDB();
    return (Boolean) configMap.getOrDefault(ALTER_PRIMARY_KEY_KEY, ALTER_PRIMARY_KEY_DEFAULT);
  }

  public boolean isEnableTableLock() throws IOException, SQLException {
    Map<String, Object> configMap = readConfMapFromTiDB();
    Object enableTableLock =
        configMap.getOrDefault(ENABLE_TABLE_LOCK_KEY, ENABLE_TABLE_LOCK_DEFAULT);
    return (Boolean) enableTableLock;
  }

  public boolean supportClusteredIndex() throws IOException, SQLException {
    try {
      queryTiDBViaJDBC("select @@tidb_enable_clustered_index");
    } catch (SQLException e) {
      return false;
    }
    return true;
  }

  /**
   * get enable-table-lock config from tidb
   *
   * @return Milliseconds
   * @throws IOException
   * @throws SQLException
   */
  public int getDelayCleanTableLock() throws IOException, SQLException {
    Map<String, Object> configMap = readConfMapFromTiDB();
    Object enableTableLock =
        configMap.getOrDefault(DELAY_CLEAN_TABLE_LOCK, DELAY_CLEAN_TABLE_LOCK_DEFAULT);
    return (int) enableTableLock;
  }

  /**
   * get row format version from tidb
   *
   * @return 1 if should not encode and write with new row format.(default) 2 if encode and write
   *     with new row format.(default on v4.0.0 cluster)
   */
  public int getRowFormatVersion() {
    try {
      List<List<Object>> result = queryTiDBViaJDBC(TIDB_ROW_FORMAT_VERSION_SQL);
      if (result.isEmpty()) {
        // default set to 1
        return TIDB_ROW_FORMAT_VERSION_DEFAULT;
      } else {
        Object version = result.get(0).get(0);
        if (version instanceof String) {
          return Integer.parseInt((String) version);
        } else if (version instanceof Number) {
          return ((Number) version).intValue();
        } else {
          return TIDB_ROW_FORMAT_VERSION_DEFAULT;
        }
      }
    } catch (Exception ignored) {
      return TIDB_ROW_FORMAT_VERSION_DEFAULT;
    }
  }

  public boolean lockTableWriteLocal(String databaseName, String tableName) throws SQLException {
    try (Statement tidbStmt = connection.createStatement()) {
      String sql = "lock tables `" + databaseName + "`.`" + tableName + "` write local";
      int result = tidbStmt.executeUpdate(sql);
      return result == 0;
    }
  }

  public boolean unlockTables() throws SQLException {
    try (Statement tidbStmt = connection.createStatement()) {
      int result = tidbStmt.executeUpdate(UNLOCK_TABLES_SQL);
      return result == 0;
    }
  }

  public boolean dropTable(String databaseName, String tableName) throws SQLException {
    try (Statement tidbStmt = connection.createStatement()) {
      String sql = "drop table if exists `" + databaseName + "`.`" + tableName + "`";
      return tidbStmt.execute(sql);
    }
  }

  public void updateTableStatistics(long startTS, long tableId, long delta, long count)
      throws SQLException {
    try (Statement tidbStmt = connection.createStatement()) {
      String sql;
      if (delta < 0) {
        sql =
            String.format(
                "update mysql.stats_meta set version = %s, count = count - %s, modify_count = modify_count + %s where table_id = %s and count >= %s",
                startTS, -delta, count, tableId, -delta);
      } else {
        sql =
            String.format(
                "update mysql.stats_meta set version = %s, count = count + %s, modify_count = modify_count + %s where table_id = %s",
                startTS, delta, count, tableId);
      }

      logger.info("updateTableStatistics: " + sql);
      tidbStmt.executeUpdate(sql);
    }
  }

  private Map<String, Object> readConfMapFromTiDB() throws SQLException, IOException {
    String configJSON = (String) queryTiDBViaJDBC(SELECT_TIDB_CONFIG_SQL).get(0).get(0);
    ObjectMapper objectMapper = new ObjectMapper();
    TypeReference<HashMap<String, Object>> typeRef =
        new TypeReference<HashMap<String, Object>>() {};
    return objectMapper.readValue(configJSON, typeRef);
  }

  public boolean isClosed() throws SQLException {
    return connection.isClosed();
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }

  private List<List<Object>> queryTiDBViaJDBC(String query) throws SQLException {
    ArrayList<List<Object>> result = new ArrayList<>();

    try (Statement tidbStmt = connection.createStatement()) {
      ResultSet resultSet = tidbStmt.executeQuery(query);
      ResultSetMetaData rsMetaData = resultSet.getMetaData();

      while (resultSet.next()) {
        ArrayList<Object> row = new ArrayList<>();
        for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
          row.add(resultSet.getObject(i));
        }
        result.add(row);
      }
    }

    return result;
  }
}
