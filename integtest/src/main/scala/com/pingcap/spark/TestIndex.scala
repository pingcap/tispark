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

/**
  * Created by birdstorm on 2017/11/5.
  */
class TestIndex(prop: Properties) extends TestCase(prop) {

  private def testType(): Unit = {
    execBothAndShow(s"select * from t1")
    var result = false
    result |= execBothAndJudge(s"select c1 from t1")
    result |= execBothAndJudge(s"select c2 from t1")
    result |= execBothAndJudge(s"select c3 from t1")
    result |= execBothAndJudge(s"select c4 from t1")
    result |= execBothAndJudge(s"select c5 from t1")
    result |= execBothAndJudge(s"select c6 from t1")
    result |= execBothAndJudge(s"select c7 from t1")
    result |= execBothAndJudge(s"select c8 from t1")
    result |= execBothAndJudge(s"select c9 from t1")
    result |= execBothAndJudge(s"select c10 from t1")
    result |= execBothAndSkip(s"select c11 from t1")
    result |= execBothAndJudge(s"select c12 from t1")
    result |= execBothAndSkip(s"select c13 from t1")
    result |= execBothAndJudge(s"select c14 from t1")
    result |= execBothAndSkip(s"select c15 from t1")

    result = !result
    logger.warn(s"\n*************** SQL Type Tests result: $result\n\n\n")
  }

  private def testTimeType(): Unit = {
    execSparkAndShow(s"select * from t2")
    execSparkAndShow(s"select * from t3")
    var result = false

    result |= execBothAndSkip(s"select UNIX_TIMESTAMP(c14) from t1")
    execSparkAndShow(s"select CAST(c14 AS LONG) from t1")
    execSparkAndShow(s"select c13 from t1")

    execTiDBAndShow(s"select c14 + c13 from t1")
    execSparkAndShow(s"select CAST(c14 AS LONG) + c13 from t1")

    result |= execBothAndSkip(s"select c15 from t1")
    result |= execBothAndSkip(s"select UNIX_TIMESTAMP(c15) from t1")

    result = !result
    logger.warn(s"\n*************** SQL Time Tests result: " + (if (result) "true" else "Fixing...Skipped") + "\n\n\n")
  }

  private def testIndex(): Unit = {
    var result = false
    result |= execBothAndJudge("select * from test_index where a < 30")

    result |= execBothAndJudge("select * from test_index where d > \'116.5\'")
    result |= execBothAndJudge("select * from test_index where d < \'116.5\'")
    result |= execBothAndJudge("select * from test_index where d > \'116.3\' and d < \'116.7\'")

    result |= execBothAndJudge("select * from test_index where d = \'116.72873\'")
    result |= execBothAndJudge("select * from test_index where d = \'116.72874\' and e < \'40.0452\'")

    result |= execBothAndJudge("select * from test_index where c > \'2008-02-06 14:00:00\'")
    result |= execBothAndJudge("select * from test_index where c >= \'2008-02-06 14:00:00\'")
    result |= execBothAndJudge("select * from test_index where c < \'2008-02-06 14:00:00\'")
    result |= execBothAndJudge("select * from test_index where c <= \'2008-02-06 14:00:00\'")
    //  TODO: this case should be fixed later
    result |= execBothAndSkip("select * from test_index where c = \'2008-02-06 14:00:00\'")

    result |= execBothAndJudge("select * from test_index where c > date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where c >= date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where c < date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where c <= date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where DATE(c) = date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where DATE(c) > date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where DATE(c) >= date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where DATE(c) < date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where DATE(c) <= date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where c <> date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where c > \'2008-02-04 14:00:00\' and d > \'116.5\'")
    result |= execBothAndJudge("select * from test_index where d = \'116.72873\' and c > \'2008-02-04 14:00:00\'")
    result |= execBothAndJudge("select * from test_index where d = \'116.72873\' and c < \'2008-02-04 14:00:00\'")

    result = !result
    logger.warn(s"\n*************** Index Tests result: $result\n\n\n")
  }

  def run(dbName: String): Unit = {
    spark.init(dbName)
    jdbc.init(dbName)
    testType()
    testTimeType()
    testIndex()
  }

}
