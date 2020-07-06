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

package org.apache.spark.sql.catalyst.plans.logical

import com.pingcap.tikv.meta.TiTimestamp
import org.apache.spark.sql.catalyst.plans.BasePlanTest

class LogicalPlanTestSuite extends BasePlanTest {
  test("fix Residual Filter containing wrong info") {
    val df = spark
      .sql("select * from full_data_type_table where tp_mediumint > 0 order by tp_int")
    if (extractDAGRequests(df).head.toString.contains("Residual Filters")) {
      fail("Residual Filters should not appear")
    }
  }

  test("test timestamp in logical plan") {
    tidbStmt.execute("DROP TABLE IF EXISTS `test1`")
    tidbStmt.execute("DROP TABLE IF EXISTS `test2`")
    tidbStmt.execute("DROP TABLE IF EXISTS `test3`")
    tidbStmt.execute(
      "CREATE TABLE `test1` (`id` int primary key, `c1` int, `c2` int, KEY idx(c1, c2))")
    tidbStmt.execute("CREATE TABLE `test2` (`id` int, `c1` int, `c2` int)")
    tidbStmt.execute("CREATE TABLE `test3` (`id` int, `c1` int, `c2` int, KEY idx(c2))")
    tidbStmt.execute(
      "insert into test1 values(1, 2, 3), /*(1, 3, 2), */(2, 2, 4), (3, 1, 3), (4, 2, 1)")
    tidbStmt.execute(
      "insert into test2 values(1, 2, 3), (1, 2, 4), (2, 1, 4), (3, 1, 3), (4, 3, 1)")
    tidbStmt.execute(
      "insert into test3 values(1, 2, 3), (2, 1, 3), (2, 1, 4), (3, 2, 3), (4, 2, 1)")
    refreshConnections()
    val df =
      spark.sql("""
                  |select t1.*, (
                  |	select count(*)
                  |	from test2
                  |	where id > 1
                  |), t1.c1, t2.c1, t3.*, t4.c3
                  |from (
                  |	select id, c1, c2
                  |	from test1) t1
                  |left join (
                  |	select id, c1, c2, c1 + coalesce(c2 % 2) as c3
                  |	from test2 where c1 + c2 > 3) t2
                  |on t1.id = t2.id
                  |left join (
                  |	select max(id) as id, min(c1) + c2 as c1, c2, count(*) as c3
                  |	from test3
                  |	where c2 <= 3 and exists (
                  |		select * from (
                  |			select id as c1 from test3)
                  |    where (
                  |      select max(id) from test1) = 4)
                  |	group by c2) t3
                  |on t1.id = t3.id
                  |left join (
                  |	select max(id) as id, min(c1) as c1, max(c1) as c1, count(*) as c2, c2 as c3
                  |	from test3
                  |	where id not in (
                  |		select id
                  |		from test1
                  |		where c2 > 2)
                  |	group by c2) t4
                  |on t1.id = t4.id
      """.stripMargin)

    var v: TiTimestamp = null

    def checkTimestamp(version: TiTimestamp): Unit =
      if (version == null) {
        fail("timestamp is not defined!")
      } else if (v == null) {
        println("initialize timestamp should be " + version.getVersion)
        v = version
      } else if (v.getVersion != version.getVersion) {
        fail("multiple timestamp found in plan")
      } else {
        println("check ok " + v.getVersion)
      }

    extractDAGRequests(df).map(_.getStartTs).foreach { checkTimestamp }
  }

  // https://github.com/pingcap/tispark/issues/1498
  test("test index scan failed to push down varchar") {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute(
      """
        |CREATE TABLE `t` (
        |  `id` int(11) NOT NULL PRIMARY KEY,
        |  `artical_id` varchar(255) DEFAULT NULL,
        |  `c` bigint(20) UNSIGNED DEFAULT NULL,
        |  `last_modify_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        |  KEY `artical_id`(`artical_id`)
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
      """.stripMargin)
    tidbStmt.execute(
      "insert into t values(1, 'abc', 18446744073709551615, '2020-06-06 00:00:00'), (2, 'abcdefg', 18446744073709551614, '2020-06-26 00:00:00'), (3, 'acdgga', 5, '2020-06-25 00:00:00'), (4, 'abcdefgh', 18446744073709551614, '2020-06-12 00:00:00'), (5, 'aaabcd', 10, '2020-06-16 00:00:00')")
    tidbStmt.execute("analyze table t")
    val df = spark.sql(
      "select artical_id from t where artical_id = 'abcdefg' and last_modify_time > '2020-06-22 00:00:00'")
    checkIsIndexScan(df, "t")
    checkIndex(df, "artical_id")
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists t")
      tidbStmt.execute("drop table if exists test1")
      tidbStmt.execute("drop table if exists test2")
      tidbStmt.execute("drop table if exists test3")
    } finally {
      super.afterAll()
    }
}
