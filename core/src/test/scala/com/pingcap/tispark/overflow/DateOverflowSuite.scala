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
    dropTable()
    jdbcUpdate(
      s"create table $dbtable(c1 DATE)"
    )

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
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test DATE Month Upper bound Overflow") {
    dropTable()
    jdbcUpdate(
      s"create table $dbtable(c1 DATE)"
    )

    val row = Row("2019-13-01")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: invalid time format: '13'"
    val tidbErrorClass = classOf[java.lang.IllegalArgumentException]
    val tidbErrorMsg = null

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
