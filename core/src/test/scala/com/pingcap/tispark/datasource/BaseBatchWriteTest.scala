/*
 * Copyright 2020 PingCAP, Inc.
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

package com.pingcap.tispark.datasource

class BaseBatchWriteTest(
    override val table: String,
    override val database: String = "tispark_test")
    extends BaseBatchWriteWithoutDropTableTest(table, database) {

  override def beforeEach(): Unit = {
    super.beforeEach()
    dropTable()
  }
}
