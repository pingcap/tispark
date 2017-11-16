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
class TestTypes(prop: Properties) extends TestCase(prop) {

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

  private def testType2(): Unit = {
    execBothAndShow(s"select * from all_data_types")
    var result = false
    result |= execBothAndJudge(s"select `varchar` from all_data_types")
    result |= execBothAndJudge(s"select `tinyint` from all_data_types")
    result |= execBothAndSkip(s"select `text` from all_data_types")
    result |= execBothAndJudge(s"select `date` from all_data_types")
    result |= execBothAndJudge(s"select `smallint` from all_data_types")
    result |= execBothAndJudge(s"select `mediumint` from all_data_types")
    result |= execBothAndJudge(s"select `int` from all_data_types")
    result |= execBothAndJudge(s"select `bigint` from all_data_types")
    result |= execBothAndJudge(s"select `float` from all_data_types")
    result |= execBothAndJudge(s"select `double` from all_data_types")
    result |= execBothAndJudge(s"select `decimal` from all_data_types")
    result |= execBothAndJudge(s"select `datetime` from all_data_types")
    result |= execBothAndJudge(s"select `timestamp` from all_data_types")
    result |= execBothAndJudge(s"select `time` from all_data_types")
    result |= execBothAndSkip(s"select `year` from all_data_types")
    result |= execBothAndJudge(s"select `char` from all_data_types")
    result |= execBothAndSkip(s"select `tinyblob` from all_data_types")
    result |= execBothAndSkip(s"select `tinytext` from all_data_types")
    result |= execBothAndSkip(s"select `blob` from all_data_types")
    result |= execBothAndSkip(s"select `mediumblob` from all_data_types")
    result |= execBothAndSkip(s"select `mediumtext` from all_data_types")
    result |= execBothAndSkip(s"select `longblob` from all_data_types")
    result |= execBothAndSkip(s"select `longtext` from all_data_types")
    result |= execBothAndJudge(s"select `enum` from all_data_types")
    result |= execBothAndJudge(s"select `set` from all_data_types")
    result |= execBothAndJudge(s"select `bool` from all_data_types")
    result |= execBothAndSkip(s"select `binary` from all_data_types")
    result |= execBothAndSkip(s"select `varbinary` from all_data_types")

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

    result |= execBothAndJudge("select `datetime` from all_data_types where `datetime` > now()")
    result |= execBothAndJudge("select `datetime` from all_data_types where `datetime` < now()")
    result |= execBothAndJudge("select `timestamp` from all_data_types where `timestamp` > now()")
    result |= execBothAndJudge("select `timestamp` from all_data_types where `timestamp` < now()")

    result |= execBothAndJudge("select `timestamp` from all_data_types where `timestamp` < now()")

    result = !result
    logger.warn(s"\n*************** SQL Time Tests result: " + (if (result) "true" else "Fixing...Skipped") + "\n\n\n")
  }

  override def run(dbName: String): Unit = {
    spark.init(dbName)
    jdbc.init(dbName)
    testType()
    testType2()
    testTimeType()
  }

}

