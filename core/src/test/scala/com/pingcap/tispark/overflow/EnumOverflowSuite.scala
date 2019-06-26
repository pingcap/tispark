package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * ENUM type include:
 * 1. ENUM
 */
class EnumOverflowSuite extends BaseDataSourceTest("test_data_type_enum_overflow") {

  test("Test ENUM Value Overflow") {
    testEnumValueOverflow(false)
  }

  test("Test ENUM as key Value Overflow") {
    testEnumValueOverflow(true)
  }

  private def testEnumValueOverflow(testKey: Boolean): Unit = {

    dropTable()
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 ENUM('male', 'female', 'both', 'unknown') primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 ENUM('male', 'female', 'both', 'unknown'))")
    }

    val row = Row("abc")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Incorrect enum value: 'abc' for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "Incorrect enum value: 'abc'"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test ENUM Number Overflow") {
    testEnumNumberOverflow(false)
  }

  test("Test ENUM as key Number Overflow") {
    testEnumNumberOverflow(true)
  }

  private def testEnumNumberOverflow(testKey: Boolean): Unit = {

    dropTable()
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 ENUM('male', 'female', 'both', 'unknown') primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 ENUM('male', 'female', 'both', 'unknown'))")
    }

    val row = Row("5")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Incorrect enum value: '5' for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 5 > upperBound 4"

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
