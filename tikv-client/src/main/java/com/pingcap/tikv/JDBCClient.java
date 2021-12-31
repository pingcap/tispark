package com.pingcap.tikv;

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
  private static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";
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
    try (Connection connection = driver.connect(url, properties);
        Statement tidbStmt = connection.createStatement();
        ResultSet resultSet = tidbStmt.executeQuery(SQL_SHOW_GRANTS)) {

      List<String> result = new ArrayList<>();
      ResultSetMetaData rsMetaData = resultSet.getMetaData();

      while (resultSet.next()) {
        for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
          result.add(resultSet.getString(i));
        }
      }

      return result;
    } catch (SQLException e) {
      return Collections.emptyList();
    }
  }

  public String getPDAddress() throws SQLException {
    try (Connection connection = driver.connect(url, properties);
        Statement tidbStmt = connection.createStatement();
        ResultSet resultSet = tidbStmt.executeQuery(GET_PD_ADDRESS)) {

      List<String> result = new ArrayList<>();
      ResultSetMetaData rsMetaData = resultSet.getMetaData();

      while (resultSet.next()) {
        for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
          result.add(resultSet.getString(i));
        }
      }
      return result.get(0);
    }
  }

  public String getCurrentUser() throws SQLException {
    try (Connection connection = driver.connect(url, properties);
        Statement tidbStmt = connection.createStatement();
        ResultSet resultSet = tidbStmt.executeQuery(SELECT_CURRENT_USER)) {

      List<String> result = new ArrayList<>();
      ResultSetMetaData rsMetaData = resultSet.getMetaData();

      while (resultSet.next()) {
        for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
          result.add(resultSet.getString(i));
        }
      }
      return result.get(0);
    }
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

  @SneakyThrows
  private Driver createDriver() {
    return (Driver) Class.forName(MYSQL_DRIVER_NAME).getDeclaredConstructor().newInstance();
  }
}
