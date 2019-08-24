package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * ENUM type include:
 * 1. ENUM
 */
class EnumOverflowSuite extends BaseDataSourceTest {

  test("Test ENUM Value Overflow") {
    testEnumValueOverflow(testKey = false, "enum_val_overflow")
  }

  test("Test ENUM as key Value Overflow") {
    testEnumValueOverflow(testKey = true, "key_enum_val_overflow")
  }

  private def testEnumValueOverflow(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 ENUM('male', 'female', 'both', 'unknown') primary key)",
        table
      )
    } else {
      createTable("create table `%s`.`%s`(c1 ENUM('male', 'female', 'both', 'unknown'))", table)
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
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test ENUM Number Overflow") {
    testEnumNumberOverflow(testKey = false, "enum_number_overflow")
  }

  test("Test ENUM as key Number Overflow") {
    testEnumNumberOverflow(testKey = true, "key_enum_number_overflow")
  }

  private def testEnumNumberOverflow(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        s"create table `%s`.`%s`(c1 ENUM('male', 'female', 'both', 'unknown') primary key)",
        table
      )
    } else {
      createTable(s"create table `%s`.`%s`(c1 ENUM('male', 'female', 'both', 'unknown'))", table)
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
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }
}
