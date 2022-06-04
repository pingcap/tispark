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

package com.pingcap.tikv.policy;

import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;
import com.pingcap.tikv.operation.ErrorHandler;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import javax.annotation.Nonnull;

public class JDBCRetryPolicy<T> {
  protected ErrorHandler<T> handler;
  protected BackOffer backOffer;

  public JDBCRetryPolicy(@Nonnull ErrorHandler<T> handler) {
    this.backOffer = ConcreteBackOffer.newCustomBackOff(10 * 1000);
    this.handler = handler;
  }

  public JDBCRetryPolicy(@Nonnull ErrorHandler<T> handler, BackOffer backOffer) {
    this.handler = handler;
    this.backOffer = backOffer;
  }

  public <RespT> RespT executeWithRetry(Callable<RespT> proc) throws SQLException {
    RespT result = null;
    while (true) {
      try {
        result = proc.call();
      } catch (CommunicationsException e) {
        boolean retry = handler.handleRequestError(backOffer, e);
        if (retry) {
          continue;
        }
      } catch (SQLException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      return result;
    }
  }
}
