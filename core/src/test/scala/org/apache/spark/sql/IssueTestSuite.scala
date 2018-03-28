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
    val project_df = join_df.select("k1", "k2", "c1")
    project_df.show
  }

  // https://github.com/pingcap/tispark/issues/272
  test("Prefix index read does not work correctly") {
    tidbStmt.execute("DROP TABLE IF EXISTS `prefix`")
    tidbStmt.execute(
      "CREATE TABLE `prefix` (\n  `a` int(11) NOT NULL,\n  `b` varchar(55) DEFAULT NULL,\n  `c` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`),\n  KEY `prefix_index` (`b`(2)),\n KEY `prefix_complex` (`a`, `b`(2))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
    )
    tidbStmt.execute(
      "INSERT INTO `prefix` VALUES(1, \"bbb\", 3), (2, \"bbc\", 4), (3, \"bbb\", 5), (4, \"abc\", 6), (5, \"abc\", 7), (6, \"abc\", 7)"
    )
    tidbStmt.execute("ANALYZE TABLE `prefix`")
    refreshConnections()
    // add explain to show if we have actually used prefix index in plan
    spark.sql("select a, b from prefix where b < \"bbc\"").explain
    spark.sql("select a, b from prefix where a = 1 and b = \"bbb\"").explain
    spark.sql("select b from prefix where b = \"bbc\"").explain
    spark.sql("select b from prefix where b >= \"bbc\" and b < \"bbd\"").explain
    spark.sql("select c, b from prefix where b > \"bb\" and b < \"bbc\"").explain
    assert(execDBTSAndJudge("select a, b from prefix where b < \"bbc\""))
    assert(execDBTSAndJudge("select a, b from prefix where a = 1 and b = \"bbb\""))
    assert(execDBTSAndJudge("select b from prefix where b = \"bbc\""))
    assert(execDBTSAndJudge("select b from prefix where b >= \"bbc\" and b < \"bbd\""))
    assert(execDBTSAndJudge("select c, b from prefix where b = \"bb\" and b < \"bbc\""))
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
    runTest(q1, q1.replace("full_data_type_table", "full_data_type_table_j"))
    runTest(q2, q2.replace("full_data_type_table", "full_data_type_table_j"))
  }

  // https://github.com/pingcap/tikv-client-lib-java/issues/198
  test("Default value information not fetched") {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("create table t(c1 int default 1)")
    tidbStmt.execute("insert into t values()")
    tidbStmt.execute("insert into t values(0)")
    tidbStmt.execute("insert into t values(null)")
    refreshConnections() // refresh since we need to load data again
    assert(execDBTSAndJudge("select * from t"))
    tidbStmt.execute("alter table t add column c2 int default null")
    refreshConnections()
    assert(execDBTSAndJudge("select * from t"))
    tidbStmt.execute("alter table t drop column c2")
    refreshConnections()
    assert(execDBTSAndJudge("select * from t"))
    tidbStmt.execute("alter table t add column c2 int default 3")
    refreshConnections()
    assert(execDBTSAndJudge("select * from t"))
  }

  // https://github.com/pingcap/tispark/issues/162
  test("select count(something + constant) reveals NPE on master branch") {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("create table t(c1 int not null)")
    tidbStmt.execute("insert into t values(1)")
    tidbStmt.execute("insert into t values(2)")
    tidbStmt.execute("insert into t values(4)")
    refreshConnections() // refresh since we need to load data again
    assert(execDBTSAndJudge("select count(c1) from t"))
    assert(execDBTSAndJudge("select count(c1 + 1) from t"))
    assert(execDBTSAndJudge("select count(1 + c1) from t"))
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("create table t(c1 int not null, c2 int not null)")
    tidbStmt.execute("insert into t values(1, 4)")
    tidbStmt.execute("insert into t values(2, 2)")
    refreshConnections()
    assert(execDBTSAndJudge("select count(c1 + c2) from t"))
  }

  override def afterAll(): Unit = {
    try {
      tidbStmt.execute("drop table if exists t")
      tidbStmt.execute("drop table if exists tmp_debug")
      tidbStmt.execute("drop table if exists prefix")
      tidbStmt.execute("drop table if exists t1")
      tidbStmt.execute("drop table if exists t2")
    } finally {
      super.afterAll()
    }
  }
}
