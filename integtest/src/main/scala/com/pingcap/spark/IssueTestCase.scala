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

package com.pingcap.spark

import java.util.Properties

import scala.collection.mutable.ArrayBuffer

class IssueTestCase(prop: Properties) extends TestCase(prop) {
  private var databaseName: String = ""

  /**
   * An example of Tests for issues
   * Use "Test"+"Client"/"Spark"+<IssueNumber> for test name.
   * Common test includes building test table from TiDB
   * and retrieving result from TiSpark.
   * Remember to call refresh() after test data is built or renewed.
   *
   * @param dbName Name of database, the default is issue_test
   */
  private def TestClient0198(dbName: String): Unit = {
    var result = false
    jdbc.execTiDB(s"use $dbName")
    jdbc.execTiDB("drop table if exists t")
    jdbc.execTiDB("create table t(c1 int default 1)")
    jdbc.execTiDB("insert into t values()")
    jdbc.execTiDB("insert into t values(0)")
    jdbc.execTiDB("insert into t values(null)")
    refresh() // refresh since we need to load data again
    result |= execBothAndJudge("select * from t")
    jdbc.execTiDB("alter table t add column c2 int default null")
    refresh()
    result |= execBothAndJudge("select * from t")
    jdbc.execTiDB("alter table t drop column c2")
    refresh()
    result |= execBothAndJudge("select * from t")
    jdbc.execTiDB("alter table t add column c2 int default 3")
    refresh()
    result |= execBothAndJudge("select * from t")
    result = !result
    logger.warn(s"\n*************** Issue Client#0198 Tests result: $result\n\n\n")
  }

  private def TestSpark0162(dbName: String): Unit = {
    var result = false
    jdbc.execTiDB(s"use $dbName")
    jdbc.execTiDB("drop table if exists t")
    jdbc.execTiDB("create table t(c1 int not null)")
    jdbc.execTiDB("insert into t values(1)")
    jdbc.execTiDB("insert into t values(2)")
    jdbc.execTiDB("insert into t values(4)")
    refresh() // refresh since we need to load data again
    result |= execBothAndJudge("select count(c1) from t")
    result |= execBothAndJudge("select count(c1 + 1) from t")
    result |= execBothAndJudge("select count(1 + c1) from t")
    jdbc.execTiDB("drop table if exists t")
    jdbc.execTiDB("create table t(c1 int not null, c2 int not null)")
    jdbc.execTiDB("insert into t values(1, 4)")
    jdbc.execTiDB("insert into t values(2, 2)")
    refresh()
    result |= execBothAndJudge("select count(c1 + c2) from t")
    result = !result
    logger.warn(s"\n*************** Issue Spark#0162 Tests result: $result\n\n\n")
  }

  private def refresh(): Unit = {
    spark.init(databaseName)
    spark_jdbc.init(databaseName)
    jdbc.init(databaseName)
  }

  override def run(dbName: String, testCases: ArrayBuffer[(String, String)]): Unit = {
    databaseName = dbName
    TestClient0198(dbName)
    TestSpark0162(dbName)
  }

}
