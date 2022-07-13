/*
 * Copyright 2021 PingCAP, Inc.
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

package com.pingcap.tikv.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCClient {

  public static final String SQL_SHOW_GRANTS = "SHOW GRANTS";
  public static final String GET_PD_ADDRESS =
      "SELECT `INSTANCE` FROM `INFORMATION_SCHEMA`.`CLUSTER_INFO` WHERE `TYPE` = 'pd'";
  private static final String MYSQL_DRIVER_NAME = "org.mariadb.jdbc.Driver";
  private static final String SQL_SHOW_GRANTS_USING_ROLE = "SHOW GRANTS FOR CURRENT_USER USING ";
  private static final String SELECT_CURRENT_USER = "SELECT CURRENT_USER()";
  private final Logger logger = LoggerFactory.getLogger(getClass().getName());

  private final String url;

  private final Driver driver;

  private final Properties properties;

  public JDBCClient(String url, Properties properties) {
    this.url = url;
    this.properties = properties;
    this.driver = createDriver();
  }

  public List<String> showGrants() {
    try {
      return query(SQL_SHOW_GRANTS, (rs, rowNum) -> rs.getString(1));
    } catch (SQLException e) {
      return Collections.emptyList();
    }
  }

  public String getPDAddress() throws SQLException {
    return queryForObject(GET_PD_ADDRESS, (rs, rowNum) -> rs.getString(1));
  }

  public String getCurrentUser() throws SQLException {
    return queryForObject(SELECT_CURRENT_USER, (rs, rowNum) -> rs.getString(1));
  }

  public List<String> showGrantsUsingRole(List<String> roles) {
    StringBuilder builder = new StringBuilder(SQL_SHOW_GRANTS_USING_ROLE);
    for (int i = 0; i < roles.size(); i++) {
      builder.append("?").append(",");
    }
    String statement = builder.deleteCharAt(builder.length() - 1).toString();

    try (Connection connection = driver.connect(url, properties);
        PreparedStatement tidbStmt = connection.prepareStatement(statement)) {
      List<String> result = new ArrayList<>();
      for (int i = 0; i < roles.size(); i++) {
        tidbStmt.setString(i + 1, roles.get(i));
      }
      ResultSet resultSet = tidbStmt.executeQuery();
      ResultSetMetaData rsMetaData = resultSet.getMetaData();

      while (resultSet.next()) {
        for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
          result.add(resultSet.getString(i));
        }
      }
      return result;
    } catch (SQLException e) {
      logger.warn("Failed to show grants using role", e);
      return Collections.emptyList();
    }
  }

  private <T> List<T> query(String sql, RowMapper<T> rowMapper) throws SQLException {
    try (Connection connection = driver.connect(url, properties);
        Statement tidbStmt = connection.createStatement();
        ResultSet resultSet = tidbStmt.executeQuery(sql)) {
      return extractData(resultSet, rowMapper);
    }
  }

  private <T> T queryForObject(String sql, RowMapper<T> rowMapper) throws SQLException {
    List<T> result = query(sql, rowMapper);
    if (result.size() != 1) {
      throw new IllegalArgumentException(
          "queryForObject() result size: expected 1, acctually " + result.size());
    }
    return result.get(0);
  }

  private <T> List<T> extractData(ResultSet rs, RowMapper<T> rowMapper) throws SQLException {
    List<T> results = new ArrayList<>();
    int rowNum = 0;
    while (rs.next()) {
      results.add(rowMapper.mapRow(rs, rowNum++));
    }
    return results;
  }

  @SneakyThrows
  private Driver createDriver() {
    return (Driver) Class.forName(MYSQL_DRIVER_NAME).getDeclaredConstructor().newInstance();
  }
}
