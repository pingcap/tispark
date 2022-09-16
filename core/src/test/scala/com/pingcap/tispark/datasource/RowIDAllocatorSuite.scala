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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.datasource

import com.pingcap.tikv.allocator.RowIDAllocator
import org.apache.spark.sql.BaseTiSparkTest

class RowIDAllocatorSuite extends BaseTiSparkTest {
  test("test unsigned allocator") {
    tidbStmt.execute("drop table if exists rowid_allocator")
    tidbStmt.execute("""CREATE TABLE `rowid_allocator` (
                       |  `a` int(11) DEFAULT NULL
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
      """.stripMargin)

    val dbName = dbPrefix + "tispark_test"
    val tableName = "rowid_allocator"
    val tiDBInfo = ti.clientSession.getCatalog.getDatabase(dbName)
    val tiTableInfo =
      ti.clientSession.getCatalog.getTable(dbName, tableName)
    // corner case allocate unsigned long's max value.
    val allocator =
      RowIDAllocator.create(tiDBInfo.getId, tiTableInfo, ti.clientSession.getConf, true, -2L)
    assert(allocator.getEnd - allocator.getStart == -2L)
  }

  test("test signed allocator") {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("""CREATE TABLE `t` (
                       |  `a` int(11) DEFAULT NULL
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
      """.stripMargin)

    val dbName = dbPrefix + "tispark_test"
    val tableName = "t"
    val tiDBInfo = ti.clientSession.getCatalog.getDatabase(dbName)
    val tiTableInfo =
      ti.clientSession.getCatalog.getTable(dbName, tableName)
    // first
    var allocator =
      RowIDAllocator.create(tiDBInfo.getId, tiTableInfo, ti.clientSession.getConf, false, 1000)
    assert(allocator.getEnd - allocator.getStart == 1000)

    // second
    allocator = RowIDAllocator
      .create(tiDBInfo.getId, tiTableInfo, ti.clientSession.getConf, false, 10000)
    assert(allocator.getEnd - allocator.getStart == 10000)

    // third
    allocator =
      RowIDAllocator.create(tiDBInfo.getId, tiTableInfo, ti.clientSession.getConf, false, 1000)
    assert(allocator.getEnd - allocator.getStart == 1000)
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists t")
      tidbStmt.execute("drop table if exists rowid_allocator")
    } finally {
      super.afterAll()
    }

}
