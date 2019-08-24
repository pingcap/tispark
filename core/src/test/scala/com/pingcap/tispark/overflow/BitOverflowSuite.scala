package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * BIT type include:
 * 1. BIT
 */
class BitOverflowSuite extends BaseDataSourceTest {

  test("Test BIT(1) Upper bound Overflow") {
    testBit1UpperBound(testKey = false, "bit1_upper_bound_overflow")
  }
  test("Test BIT(1) as key Upper bound Overflow") {
    testBit1UpperBound(testKey = true, "key_bit1_upper_bound_overflow")
  }

  private def testBit1UpperBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable("create table `%s`.`%s`(c1 BIT(1) primary key)", table)
    } else {
      createTable(
        "create table `%s`.`%s`(c1 BIT(1))",
        table
      )
    }

    val row = Row(2.toByte)
    val schema = StructType(
      List(
        StructField("c1", ByteType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 2 > upperBound 2"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true
    )
  }

  test("Test BIT(1) Lower bound Overflow") {
    testBit1LowerBound(testKey = false, "bit1_lower_bound_overflow")
  }

  test("Test BIT(1) as key Lower bound Overflow") {
    testBit1LowerBound(testKey = true, "key_bit1_lower_bound_overflow")
  }

  private def testBit1LowerBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 BIT(1) primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 BIT(1))",
        table
      )
    }

    val row = Row((-1).toByte)
    val schema = StructType(
      List(
        StructField("c1", ByteType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value -1 < lowerBound 0"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true
    )
  }

  test("Test BIT(4) Upper bound Overflow") {
    testBit4UpperBound(testKey = false, "bit4_upper_bound_overflow")
  }

  test("Test BIT(4) as key Upper bound Overflow") {
    testBit4UpperBound(testKey = true, "key_bit4_upper_bound_overflow")
  }

  private def testBit4UpperBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 BIT(4) primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 BIT(4))",
        table
      )
    }

    val row = Row(16.toByte)
    val schema = StructType(
      List(
        StructField("c1", ByteType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 16 > upperBound 16"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true
    )
  }

  test("Test BIT(4) Lower bound Overflow") {
    testBit4LowerBound(testKey = false, "bit4_lower_bound_overflow")
  }

  test("Test BIT(4) as key Lower bound Overflow") {
    testBit4LowerBound(testKey = true, "key_bit4_lower_bound_overflow")
  }

  private def testBit4LowerBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 BIT(4) primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 BIT(4))",
        table
      )
    }

    val row = Row((-1).toByte)
    val schema = StructType(
      List(
        StructField("c1", ByteType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value -1 < lowerBound 0"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true
    )
  }

  test("Test BIT(8) Upper bound Overflow") {
    testBit8UpperBound(testKey = false, "bit8_upper_bound_overflow")
  }

  test("Test BIT(8) as key Upper bound Overflow") {
    testBit8UpperBound(testKey = true, "bit8_upper_bound_overflow")
  }

  private def testBit8UpperBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 BIT(8) primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 BIT(8))",
        table
      )
    }

    val row = Row(256L)
    val schema = StructType(
      List(
        StructField("c1", LongType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 256 > upperBound 256"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true
    )
  }

  test("Test BIT(8) Lower bound Overflow") {
    testBit8LowerBound(testKey = false, "bit8_lower_bound_overflow")
  }

  test("Test BIT(8) as key Lower bound Overflow") {
    testBit8LowerBound(testKey = true, "key_bit8_lower_bound_overflow")
  }

  private def testBit8LowerBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 BIT(8) primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 BIT(8))",
        table
      )
    }

    val row = Row(-1L)
    val schema = StructType(
      List(
        StructField("c1", LongType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value -1 < lowerBound 0"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true
    )
  }
}
