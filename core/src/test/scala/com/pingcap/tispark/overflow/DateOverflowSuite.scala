package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * DATE type include:
 * 1. DATE
 */
class DateOverflowSuite extends BaseDataSourceTest {

  test("Test DATE YEAR Upper bound Overflow") {
    testYearOverflow(testKey = false, "date_year_upper_bound_overflow")
  }

  test("Test DATE as key YEAR Upper bound Overflow") {
    testYearOverflow(testKey = true, "key_date_year_upper_bound_overflow")
  }

  private def testYearOverflow(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 DATE primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 DATE)",
        table
      )
    }

    val row = Row("10000-01-01")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: invalid time format: '{10000 1 1 0 0 0 0}'"
    val tidbErrorClass = classOf[java.lang.IllegalArgumentException]
    val tidbErrorMsg = null

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

  test("Test DATE Month Upper bound Overflow") {
    testMonthOverflow(testKey = false, "date_month_upper_bound_overflow")
  }

  test("Test DATE as key Month Upper bound Overflow") {
    testMonthOverflow(testKey = true, "key_date_month_upper_bound_overflow")
  }

  private def testMonthOverflow(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 DATE primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 DATE)",
        table
      )
    }

    val row = Row("2019-13-01")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsgStart = "Data truncation"
    val tidbErrorClass = classOf[java.lang.IllegalArgumentException]
    val tidbErrorMsgStart = null

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      table,
      jdbcErrorClass,
      jdbcErrorMsgStart,
      tidbErrorClass,
      tidbErrorMsgStart,
      msgStartWith = true
    )
  }
}
