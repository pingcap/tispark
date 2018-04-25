/*
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
 */

package org.apache.spark.sql

import org.apache.spark.sql.functions.{col, sum}

class IssueTestSuite extends BaseTiSparkSuite {

  test("TISPARK-21 count(1) when single read results in DAGRequestException") {
    tidbStmt.execute("DROP TABLE IF EXISTS `single_read`")
    tidbStmt.execute(
      """CREATE TABLE `single_read` (
        |  `c1` int(11) NOT NULL,
        |  `c2` int(11) NOT NULL,
        |  `c3` int(11) NOT NULL,
        |  `c4` int(11) NOT NULL,
        |  `c5` int(11) DEFAULT NULL,
        |  PRIMARY KEY (`c3`,`c2`),
        |  KEY `c4` (`c4`)
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin""".stripMargin
    )
    tidbStmt.execute(
      "insert into single_read values(1, 1, 1, 2, null), (1, 2, 1, 1, null), (2, 1, 3, 2, null), (2, 2, 2, 1, 0)"
    )
    refreshConnections()

    judge("select count(1) from single_read")
    judge("select count(c1) from single_read")
    judge("select count(c2) from single_read")
    judge("select count(c5) from single_read")
    judge("select count(1) from single_read where c2 < 2")
    judge("select c2, c3 from single_read")
    judge("select c3, c4 from single_read")
  }

  test("TISPARK-16 fix excessive dag column") {
    tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
    tidbStmt.execute("DROP TABLE IF EXISTS `t2`")
    tidbStmt.execute(
      """CREATE TABLE `t1` (
        |         `c1` bigint(20) DEFAULT NULL,
        |         `k2` int(20) DEFAULT NULL,
        |         `k1` varchar(32) DEFAULT NULL
        |         ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin""".stripMargin
    )
    tidbStmt.execute(
      """CREATE TABLE `t2` (
        |         `c2` bigint(20) DEFAULT NULL,
        |         `k2` int(20) DEFAULT NULL,
        |         `k1` varchar(32) DEFAULT NULL
        |         ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin""".stripMargin
    )
    tidbStmt.execute("insert into t1 values(1, 201707, 'aa')")
    tidbStmt.execute("insert into t2 values(2, 201707, 'aa')")
    refreshConnections()

    // Note: Left outer join for DataSet is different from that in mysql.
    // The result of DataSet[a, b, c] left outer join DataSet[d, b, c]
    // on join key(b, c) will be DataSet[b, c, a, d]
    val t1_df = spark.sql("select * from t1")
    val t1_group_df = t1_df.groupBy("k1", "k2").agg(sum("c1").alias("c1"))
    val t2_df = spark.sql("select * from t2")
    t2_df.printSchema()
    t2_df.show
    val join_df = t1_group_df.join(t2_df, Seq("k1", "k2"), "left_outer")
    join_df.printSchema()
    join_df.show
    val filter_df = join_df.filter(col("c2").isNotNull)
    filter_df.show
    val project_df = join_df.select("k1", "k2", "c2")
    project_df.show
  }

  // https://github.com/pingcap/tispark/issues/262
  test("NPE when decoding datetime,date,timestamp") {
    tidbStmt.execute("DROP TABLE IF EXISTS `tmp_debug`")
    tidbStmt.execute(
      "CREATE TABLE `tmp_debug` (\n  `tp_datetime` datetime DEFAULT NULL, `tp_date` date DEFAULT NULL, `tp_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
    )
    tidbStmt.execute(
      "INSERT INTO `tmp_debug` VALUES ('0000-00-00 00:00:00','0000-00-00','0000-00-00 00:00:00')"
    )
    refreshConnections()
    spark.sql("select * from tmp_debug").collect()
  }

  // https://github.com/pingcap/tispark/issues/255
  test("Group by with first") {
    ti.tidbMapDatabase("tpch_test")
    val q1 =
      """
        |select
        |   l_returnflag
        |from
        |   lineitem
        |where
        |   l_shipdate <= date '1998-12-01'
        |group by
        |   l_returnflag""".stripMargin
    val q2 =
      """
        |select
        |   avg(l_quantity)
        |from
        |   lineitem
        |where
        |   l_shipdate >= date '1994-01-01'
        |group by
        |   l_partkey""".stripMargin
    // Should not throw any exception
    runTest(q1, skipTiDB = true)
    runTest(q2, skipTiDB = true)
  }

  // https://github.com/pingcap/tispark/issues/162
  test("select count(something + constant) reveals NPE on master branch") {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("create table t(c1 int not null)")
    tidbStmt.execute("insert into t values(1)")
    tidbStmt.execute("insert into t values(2)")
    tidbStmt.execute("insert into t values(4)")
    refreshConnections() // refresh since we need to load data again
    judge("select count(c1) from t")
    judge("select count(c1 + 1) from t")
    judge("select count(1 + c1) from t")
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("create table t(c1 int not null, c2 int not null)")
    tidbStmt.execute("insert into t values(1, 4)")
    tidbStmt.execute("insert into t values(2, 2)")
    refreshConnections()
    judge("select count(c1 + c2) from t")
  }

  override def afterAll(): Unit = {
    try {
      tidbStmt.execute("drop table if exists t")
      tidbStmt.execute("drop table if exists tmp_debug")
      tidbStmt.execute("drop table if exists t1")
      tidbStmt.execute("drop table if exists t2")
      tidbStmt.execute("drop table if exists single_read")
    } finally {
      super.afterAll()
    }
  }
}
