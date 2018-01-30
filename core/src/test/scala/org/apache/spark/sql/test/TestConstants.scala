/*
 *
 * Copyright 2017 PingCAP, Inc.
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
 *
 */

package org.apache.spark.sql.test

object TestConstants {
  val KeyTiDBAddress = "tidb.addr"
  val KeyTiDBPort = "tidb.port"
  val KeyTiDBUser = "tidb.user"
  val KeyTestDB = "test.db"
  val KeyTPCHDB = "tpch.db"
  val KeyUseRawSparkMySql = "spark.use_raw_mysql"
  val KeyMysqlAddress = "mysql.addr"
  val KeyMysqlUser = "mysql.user"
  val KeyMysqlPassword = "mysql.password"
  val KeyShouldLoadData = "test.data.load"
}
