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
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TiDBJDBCClient implements AutoCloseable {
  private Connection connection;

  private static final String UNLOCK_TABLES_SQL = "unlock tables";
  private static final String SELECT_TIDB_CONFIG_SQL = "select @@tidb_config";
  private static final String ENABLE_TABLE_LOCK_KEY = "enable-table-lock";
  private static final Boolean ENABLE_TABLE_LOCK_DEFAULT = false;

  public TiDBJDBCClient(Connection connection) {
    this.connection = connection;
  }

  public boolean isEnableTableLock() throws IOException, SQLException {
    String configJSON = (String) queryTiDBViaJDBC(SELECT_TIDB_CONFIG_SQL).get(0).get(0);
    ObjectMapper objectMapper = new ObjectMapper();
    TypeReference<HashMap<String, Object>> typeRef =
        new TypeReference<HashMap<String, Object>>() {};
    HashMap<String, Object> configMap = objectMapper.readValue(configJSON, typeRef);
    Object enableTableLock =
        configMap.getOrDefault(ENABLE_TABLE_LOCK_KEY, ENABLE_TABLE_LOCK_DEFAULT);
    return (Boolean) enableTableLock;
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

  // SPLIT TABLE table_name [INDEX index_name] BETWEEN (lower_value) AND (upper_value) REGIONS
  // region_num
  public boolean splitTableRegion(
      String dbName, String tblName, long minVal, long maxVal, long regionNum) throws SQLException {
    try (Statement tidbStmt = connection.createStatement()) {
      String sql =
          String.format(
              "split table %s.%s between (%d) and (%d) regions %d",
              dbName, tblName, minVal, maxVal, regionNum);
      return tidbStmt.execute(sql);
    }
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
