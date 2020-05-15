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
  val TiDB_ADDRESS = "tidb.addr"
  val TiDB_PORT = "tidb.port"
  val TiDB_USER = "tidb.user"
  val TiDB_PASSWORD = "tidb.password"
  val TPCH_DB_NAME = "tpch.db"
  val TPCDS_DB_NAME = "tpcds.db"
  val SHOULD_LOAD_DATA = "test.data.load"
  val SHOULD_GENERATE_DATA = "test.data.generate"
  val GENERATE_DATA_SEED = "test.data.generate.seed"
  val ENABLE_TIFLASH_TEST = "test.tiflash.enable"
  val SHOULD_SKIP_TEST = "test.skip"
}
