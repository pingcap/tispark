package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * DATE type include:
 * 1. DATE
 */
class DateOverflowSuite extends BaseDataSourceTest("test_data_type_date_overflow") {

  test("Test DATE YEAR Upper bound Overflow") {
    testYearOverflow(false)
  }

  test("Test DATE as key YEAR Upper bound Overflow") {
    testYearOverflow(true)
  }

  private def testYearOverflow(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 DATE primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 DATE)"
      )
    }

    val row = Row("10000-01-01")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[java.lang.IllegalArgumentException]
    val tidbErrorMsg = null

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test DATE Month Upper bound Overflow") {
    testMonthOverflow(false)
  }

  test("Test DATE as key Month Upper bound Overflow") {
    testMonthOverflow(true)
  }

  private def testMonthOverflow(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 DATE primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 DATE)"
      )
    }

    val row = Row("2019-13-01")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[java.lang.IllegalArgumentException]
    val tidbErrorMsgStart = null

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsgStart,
      msgStartWith = true
    )
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
