package com.pingcap.spark

import java.util.Properties

/**
  * Created by birdstorm on 2017/11/15.
  * The following tests will be merged into DAGTests
  */
@Deprecated
class TestNull(prop: Properties) extends TestCase(prop) {

  private def testNull(): Unit = {
    execBothAndShow(s"select * from all_nullable_data_types")
    var result = false
    result |= execBothAndJudge(s"select `varchar` from all_nullable_data_types")
    result |= execBothAndJudge(s"select `tinyint` from all_nullable_data_types")
    result |= execBothAndSkip(s"select `text` from all_nullable_data_types")
    result |= execBothAndJudge(s"select `date` from all_nullable_data_types")
    result |= execBothAndJudge(s"select `smallint` from all_nullable_data_types")
    result |= execBothAndJudge(s"select `mediumint` from all_nullable_data_types")
    result |= execBothAndJudge(s"select `int` from all_nullable_data_types")
    result |= execBothAndJudge(s"select `bigint` from all_nullable_data_types")
    result |= execBothAndJudge(s"select `float` from all_nullable_data_types")
    result |= execBothAndJudge(s"select `double` from all_nullable_data_types")
    result |= execBothAndJudge(s"select `decimal` from all_nullable_data_types")
    result |= execBothAndJudge(s"select `datetime` from all_nullable_data_types")
    result |= execBothAndJudge(s"select `time` from all_nullable_data_types")
    result |= execBothAndJudge(s"select `char` from all_nullable_data_types")
    result |= execBothAndSkip(s"select `tinyblob` from all_nullable_data_types")
    result |= execBothAndSkip(s"select `tinytext` from all_nullable_data_types")
    result |= execBothAndSkip(s"select `blob` from all_nullable_data_types")
    result |= execBothAndSkip(s"select `mediumblob` from all_nullable_data_types")
    result |= execBothAndSkip(s"select `mediumtext` from all_nullable_data_types")
    result |= execBothAndSkip(s"select `longblob` from all_nullable_data_types")
    result |= execBothAndSkip(s"select `longtext` from all_nullable_data_types")
    result |= execBothAndJudge(s"select `enum` from all_nullable_data_types")
    result |= execBothAndJudge(s"select `set` from all_nullable_data_types")
    result |= execBothAndJudge(s"select `bool` from all_nullable_data_types")
    result |= execBothAndSkip(s"select `binary` from all_nullable_data_types")
    result |= execBothAndSkip(s"select `varbinary` from all_nullable_data_types")

    result = !result
    logger.warn(s"\n*************** NULL Tests result: $result\n\n\n")
  }

  private def testConditions(): Unit = {
    var result = false
    result |= execBothAndJudge(s"select `int` from all_nullable_data_types where `int` = null")
    result |= execBothAndJudge(s"select `int` from all_nullable_data_types where `int` <> null")
    result |= execBothAndJudge(s"select `int` from all_nullable_data_types where `int` in (null)")
    result |= execBothAndJudge(s"select `int` from all_nullable_data_types where `int` not in (null)")
    result |= execBothAndJudge(s"select `int` from all_nullable_data_types where `int` is null")
    result |= execBothAndJudge(s"select `int` from all_nullable_data_types where `int` = 3000000")
    result |= execBothAndJudge(s"select `int` from all_nullable_data_types where `int` <= 3000000")
    result |= execBothAndJudge(s"select `int` from all_nullable_data_types where `int` >= 3000000")
    result |= execBothAndJudge(s"select `int` from all_nullable_data_types where `int` between 0 and 5000000")
    result |= execBothAndJudge(s"select `int` from all_nullable_data_types where `int` not between 20 and 50")
    result |= execBothAndJudge(s"select `int` from all_nullable_data_types where `int` in (null, 3000000)")

  }

  override def run(dbName: String): Unit = {
    spark.init(dbName)
    jdbc.init(dbName)
    testNull()
    testConditions()
  }
}
