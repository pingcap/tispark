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
  private static final String ENABLE_SPLIT_TABLE_KEY = "split-table";
  private static final Boolean ENABLE_SPLIT_TABLE_DEFAULT = false;
  private static final String TIDB_ROW_FORMAT_VERSION_SQL = "select @@tidb_row_format_version";
  private static final String TIDB_SET_WAIT_SPLIT_REGION_FINISH =
      "set @@tidb_wait_split_region_finish=%d;";
  private static final int TIDB_ROW_FORMAT_VERSION_DEFAULT = 1;
  private final Logger logger = LoggerFactory.getLogger(getClass().getName());
  private final Connection connection;
  private final int waitSplitRegionFinish;

  public TiDBJDBCClient(Connection connection) {
    this.connection = connection;
    this.waitSplitRegionFinish = 1;
  }

  public TiDBJDBCClient(Connection connection, int waitSplitRegionFinish) {
    this.connection = connection;
    this.waitSplitRegionFinish = waitSplitRegionFinish;
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

  private void setTiDBWriteSplitRegionFinish() throws SQLException {
    if (waitSplitRegionFinish == 0 || waitSplitRegionFinish == 1) {
      executeUpdate(String.format(TIDB_SET_WAIT_SPLIT_REGION_FINISH, waitSplitRegionFinish));
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

  /**
   * split table region by calling tidb jdbc command `SPLIT TABLE`, e.g. SPLIT TABLE table_name
   * BETWEEN (lower_value) AND (upper_value) REGIONS 9
   *
   * @param dbName database name in tidb
   * @param tblName table name in tidb
   * @param minVal min value
   * @param maxVal max value
   * @param regionNum number of regions to split
   */
  public void splitTableRegion(
      String dbName, String tblName, long minVal, long maxVal, long regionNum) throws SQLException {

    setTiDBWriteSplitRegionFinish();

    if (minVal < maxVal) {
      try (Statement tidbStmt = connection.createStatement()) {
        String sql =
            String.format(
                "split table `%s`.`%s` between (%d) and (%d) regions %d",
                dbName, tblName, minVal, maxVal, regionNum);
        logger.warn("split table region: " + sql);
        tidbStmt.execute(sql);
      } catch (SQLException e) {
        logger.warn("failed to split table region", e);
        throw e;
      }
    } else {
      logger.warn("try to split table region with minVal >= maxVal, skipped");
    }
  }

  public void splitIndexRegion(String dbName, String tblName, String idxName, String valueList)
      throws SQLException {

    setTiDBWriteSplitRegionFinish();

    try (Statement tidbStmt = connection.createStatement()) {
      String sql =
          String.format(
              "split table `%s`.`%s` index `%s` by %s", dbName, tblName, idxName, valueList);
      logger.warn("split index region: " + sql);
      tidbStmt.execute(sql);
    } catch (SQLException e) {
      logger.warn("failed to split index region", e);
      throw e;
    }
  }
  /**
   * split index region by calling tidb jdbc command `SPLIT TABLE`, e.g. SPLIT TABLE t INDEX idx
   * BETWEEN ("2010-01-01 00:00:00") AND ("2020-01-01 00:00:00") REGIONS 16;
   *
   * @param dbName database name in tidb
   * @param tblName table name in tidb
   * @param idxName index name in table
   * @param minIndexVal min index value
   * @param maxIndexVal max index value
   * @param regionNum number of regions to split
   */
  public void splitIndexRegion(
      String dbName,
      String tblName,
      String idxName,
      String minIndexVal,
      String maxIndexVal,
      long regionNum)
      throws SQLException {

    setTiDBWriteSplitRegionFinish();

    if (!minIndexVal.equals(maxIndexVal)) {
      try (Statement tidbStmt = connection.createStatement()) {
        String sql =
            String.format(
                "split table `%s`.`%s` index `%s` between (\"%s\") and (\"%s\") regions %d",
                dbName, tblName, idxName, minIndexVal, maxIndexVal, regionNum);
        logger.warn("split index region: " + sql);
        tidbStmt.execute(sql);
      } catch (SQLException e) {
        logger.warn("failed to split index region", e);
        throw e;
      }
    } else {
      logger.warn("try to split index region with minVal = maxVal, skipped");
    }
  }

  public boolean isClosed() throws SQLException {
    return connection.isClosed();
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }

  private int executeUpdate(String sql) throws SQLException {
    try (Statement tidbStmt = connection.createStatement()) {
      return tidbStmt.executeUpdate(sql);
    }
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
