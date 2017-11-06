package com.pingcap.spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCDataGenerator {
  public final String jdbcUsername = "root";
  public final String jdbcHostname = "localhost";
  public final String jdbcPort = "4000";
  public final String jdbcUrl = "jdbc:mysql://" + jdbcHostname + ":" + jdbcPort + "?user=" + jdbcUsername;

  public Connection connection;

  public void initConnection() throws SQLException {
    connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, "");
  }
}
