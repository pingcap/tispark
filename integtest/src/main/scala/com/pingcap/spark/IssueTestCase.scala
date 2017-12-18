package com.pingcap.spark

import java.util.Properties

import scala.collection.mutable.ArrayBuffer

/**
  * Created by birdstorm on 2017/12/15.
  */
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
    logger.warn(s"\n*************** Index Tests result: $result\n\n\n")
  }

  private def refresh(): Unit = {
    spark.init(databaseName)
    jdbc.init(databaseName)
  }

  override def run(dbName: String, testCases: ArrayBuffer[(String, String)]): Unit = {
    databaseName = dbName
    TestClient0198(dbName)
  }

}
