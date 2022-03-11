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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface RowMapper<T> {

  /**
   * Implementations must implement this method to map each row of data in the ResultSet. This
   * method should not call {@code next()} on the ResultSet; it is only supposed to map values of
   * the current row.
   *
   * @param rs the ResultSet to map (pre-initialized for the current row)
   * @param rowNum the number of the current row
   * @return the result object for the current row (may be {@code null})
   * @throws SQLException if an SQLException is encountered getting column values (that is, there's
   *     no need to catch SQLException)
   */
  T mapRow(ResultSet rs, int rowNum) throws SQLException;
}
