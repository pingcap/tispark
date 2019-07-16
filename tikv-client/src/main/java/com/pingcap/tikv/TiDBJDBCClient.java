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
import java.util.Map;

public class TiDBJDBCClient implements AutoCloseable {
  private Connection connection;

  private static final String UNLOCK_TABLES_SQL = "unlock tables";
  private static final String SELECT_TIDB_CONFIG_SQL = "select @@tidb_config";
  private static final String ENABLE_TABLE_LOCK_KEY = "enable-table-lock";
  private static final Boolean ENABLE_TABLE_LOCK_DEFAULT = false;
  private static final String DELAY_CLEAN_TABLE_LOCK = "delay-clean-table-lock";
  private static final int DELAY_CLEAN_TABLE_LOCK_DEFAULT = 0;
  private static final String ENABLE_SPLIT_TABLE_KEY = "split-table";
  private static final Boolean ENABLE_SPLIT_TABLE_DEFAULT = false;

  public TiDBJDBCClient(Connection connection) {
    this.connection = connection;
  }

  public boolean isEnableTableLock() throws IOException, SQLException {
    Map<String, Object> configMap = readConfMapFromTiDB();
    Object enableTableLock =
        configMap.getOrDefault(ENABLE_TABLE_LOCK_KEY, ENABLE_TABLE_LOCK_DEFAULT);
    return (Boolean) enableTableLock;
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

  private Map<String, Object> readConfMapFromTiDB() throws SQLException, IOException {
    String configJSON = (String) queryTiDBViaJDBC(SELECT_TIDB_CONFIG_SQL).get(0).get(0);
    ObjectMapper objectMapper = new ObjectMapper();
    TypeReference<HashMap<String, Object>> typeRef =
        new TypeReference<HashMap<String, Object>>() {};
    return objectMapper.readValue(configJSON, typeRef);
  }

  public boolean isEnableSplitRegion() throws IOException, SQLException {
    Map<String, Object> configMap = readConfMapFromTiDB();
    Object splitTable = configMap.getOrDefault(ENABLE_SPLIT_TABLE_KEY, ENABLE_SPLIT_TABLE_DEFAULT);
    return (Boolean) splitTable;
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

  /**
   * split index region by calling tidb jdbc command `SPLIT TABLE`, e.g. SPLIT TABLE t1 INDEX idx4
   * by ("a", "2000-01-01 00:00:01"), ("b", "2019-04-17 14:26:19"), ("c", ""); if you have a table
   * t1 and index idx4.
   *
   * @param dbName
   * @param tblName
   * @param idxName
   * @param splitPoints
   * @return
   * @throws SQLException
   */
  public boolean splitIndexRegion(
      String dbName, String tblName, String idxName, List<List<String>> splitPoints)
      throws SQLException {

    if (splitPoints.isEmpty()) {
      return false;
    }
    StringBuilder sb = new StringBuilder();
    sb.append("split table ")
        .append("`")
        .append(dbName)
        .append("`.`")
        .append(tblName)
        .append("`")
        .append(" index ")
        .append(idxName)
        .append(" by");

    for (int i = 0; i < splitPoints.size(); i++) {
      List<String> splitPoint = splitPoints.get(i);
      StringBuilder splitPointStr = new StringBuilder("(");
      for (int j = 0; j < splitPoint.size(); j++) {
        splitPointStr.append("\"");
        splitPointStr.append(splitPoint.get(j));
        if (j < splitPoint.size() - 1) {
          splitPointStr.append(",");
        }
        splitPointStr.append("\"");
      }
      splitPointStr.append(")");
      sb.append(splitPointStr);

      if (i < splitPoints.size() - 1) {
        sb.append(",");
      }
    }

    try (Statement tidbStmt = connection.createStatement()) {
      return tidbStmt.execute(sb.toString());
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
