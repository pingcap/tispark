/*
 * Copyright 2022 PingCAP, Inc.
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

import com.pingcap.tikv.operation.ErrorHandler;
import com.pingcap.tikv.operation.JDBCErrorHandler;
import com.pingcap.tikv.policy.JDBCRetryPolicy;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Callable;
import lombok.SneakyThrows;

public abstract class AbstractJDBCClient {

  private static final String TIDB_DRIVER_CLASS = "com.mysql.jdbc.Driver";
  private static final int CONNECTION_VALID_TIMEOUT = 1;

  protected volatile Connection connection;
  protected final String jdbcUrl;

  public AbstractJDBCClient(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  public AbstractJDBCClient(Connection connection, String jdbcUrl) {
    this.connection = connection;
    this.jdbcUrl = jdbcUrl;
  }

  @SneakyThrows
  public synchronized void updateConnection() {
    if (connection != null && connection.isValid(CONNECTION_VALID_TIMEOUT)) return;

    Driver driver =
        (Driver) Class.forName(TIDB_DRIVER_CLASS).getDeclaredConstructor().newInstance();
    if (connection != null) {
      connection.close();
    }
    connection = driver.connect(jdbcUrl, new Properties());
  }

  public <RespT> RespT executeWithUpdateConnRetry(
      Callable<RespT> method, BackOffer backOffer, ErrorHandler<RespT> errorHandler)
      throws SQLException {
    JDBCRetryPolicy<RespT> retryPolicy = new JDBCRetryPolicy<>(errorHandler, backOffer);
    return retryPolicy.executeWithRetry(
        () -> {
          if (connection == null || !connection.isValid(CONNECTION_VALID_TIMEOUT)) {
            this.updateConnection();
          }
          return method.call();
        });
  }

  public <RespT> RespT executeWithUpdateConnRetry(Callable<RespT> method) throws SQLException {
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(10 * 1000);
    ErrorHandler<RespT> errorHandler = new JDBCErrorHandler<>();
    JDBCRetryPolicy<RespT> retryPolicy = new JDBCRetryPolicy<>(errorHandler, backOffer);
    return retryPolicy.executeWithRetry(
        () -> {
          if (connection == null || !connection.isValid(CONNECTION_VALID_TIMEOUT)) {
            this.updateConnection();
          }
          return method.call();
        });
  }

  public <RespT> RespT executeWithRetry(
      Callable<RespT> method, BackOffer backOffer, ErrorHandler<RespT> errorHandler)
      throws SQLException {
    JDBCRetryPolicy<RespT> retryPolicy = new JDBCRetryPolicy<>(errorHandler, backOffer);
    return retryPolicy.executeWithRetry(() -> method.call());
  }

  public <RespT> RespT executeWithRetry(Callable<RespT> method) throws SQLException {
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(10 * 1000);
    ErrorHandler<RespT> errorHandler = new JDBCErrorHandler<>();
    JDBCRetryPolicy<RespT> retryPolicy = new JDBCRetryPolicy<>(errorHandler, backOffer);
    return retryPolicy.executeWithRetry(() -> method.call());
  }
}
