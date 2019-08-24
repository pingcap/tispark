package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * DATETIME type include:
 * 1. DATETIME
 */
class DateTimeOverflowSuite extends BaseDataSourceTest {

  test("Test DATETIME YEAR Overflow") {
    testDateTimeOverflow(testKey = false, "datetime_year_overflow")
  }

  test("Test DATETIME as key YEAR Overflow") {
    testDateTimeOverflow(testKey = true, "key_datetime_year_overflow")
  }

  private def testDateTimeOverflow(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 DATETIME(6) primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 DATETIME(6))",
        table
      )
    }

    val row = Row("10000-11-11 11:11:11")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: invalid time format: '{10000 11 11 11 11 11 0}'"
    val tidbErrorClass = classOf[java.lang.IllegalArgumentException]
    val tidbErrorMsg = "Timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff]"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }
}
