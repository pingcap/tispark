/*
 * Copyright 2019 PingCAP, Inc.
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

package org.apache.spark.sql

class PartitionTableSuite extends BaseTiSparkSuite {
  test("index scan on partition table") {
    tidbStmt.execute(
      "CREATE TABLE `pt` (   `id` int(11) DEFAULT NULL, `y` date DEFAULT NULL,   index `idx_y`(`y`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin PARTITION BY RANGE ( id ) (   PARTITION p0 VALUES LESS THAN (2),   PARTITION p1 VALUES LESS THAN (4),   PARTITION p2 VALUES LESS THAN (6) );"
    )
    tidbStmt.execute("insert into `pt` values(1, '1995-10-10')")
    tidbStmt.execute("insert into `pt` values(2, '1996-10-10')")
    tidbStmt.execute("insert into `pt` values(3, '1997-10-10')")
    tidbStmt.execute("insert into `pt` values(4, '1998-10-10')")
    tidbStmt.execute("insert into `pt` values(5, '1999-10-10')")
    refreshConnections()
    judge("select * from pt where y = date'1996-10-10'")
  }
  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists pt")
    } finally {
      super.afterAll()
    }
}
