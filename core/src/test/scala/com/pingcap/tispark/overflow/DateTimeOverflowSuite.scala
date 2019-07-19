package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * DATETIME type include:
 * 1. DATETIME
 */
class DateTimeOverflowSuite extends BaseDataSourceTest("test_data_type_datetime_overflow") {

  test("Test DATETIME YEAR Overflow") {
    testDateTimeOverflow(false)
  }

  test("Test DATETIME as key YEAR Overflow") {
    testDateTimeOverflow(true)
  }

  private def testDateTimeOverflow(testKey: Boolean): Unit = {

    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbTable(c1 DATETIME(6) primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbTable(c1 DATETIME(6))"
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
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
