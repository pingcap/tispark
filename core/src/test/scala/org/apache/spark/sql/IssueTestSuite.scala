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

class IssueTestSuite extends BaseTiSparkSuite {

  // https://github.com/pingcap/tispark/issues/262
  test("NPE when decoding datetime") {
    tidbStmt.execute("DROP TABLE IF EXISTS `tmp_debug`")
    tidbStmt.execute("CREATE TABLE `tmp_debug` (\n  `sign_time` datetime DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;")
    tidbStmt.execute("INSERT INTO `tmp_debug` VALUES ('0000-00-00 00:00:00')")
    refreshConnections()
    assert(execDBTSAndJudge("select * from tmp_debug"))
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
    } finally {
      super.afterAll()
    }
  }
}
