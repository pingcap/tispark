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

package com.pingcap.tikv.operation;

import com.pingcap.tikv.TiDBJDBCClient;
import com.pingcap.tikv.util.BackOffFunction;
import com.pingcap.tikv.util.BackOffer;

public class JDBCErrorHandler<RespT> implements ErrorHandler<RespT> {
  private final TiDBJDBCClient tiDBJDBCClient;

  public JDBCErrorHandler(TiDBJDBCClient tiDBJDBCClient) {
    this.tiDBJDBCClient = tiDBJDBCClient;
  }

  @Override
  public boolean handleResponseError(BackOffer backOffer, RespT resp) {
    return false;
  }

  @Override
  public boolean handleRequestError(BackOffer backOffer, Exception e) {
    backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoJdbcConn, e);
    return true;
  }
}
