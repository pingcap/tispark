package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * BIT type include:
 * 1. BIT
 */
class BitOverflowSuite extends BaseDataSourceTest("test_data_type_bit_overflow") {

  test("Test BIT(1) Upper bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testBit1UpperBound(false)
  }
  test("Test BIT(1) as key Upper bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testBit1UpperBound(true)
  }

  private def testBit1UpperBound(testKey: Boolean): Unit = {

    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 BIT(1) primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 BIT(1))"
      )
    }

    val row = Row(2.toByte)
    val schema = StructType(
      List(
        StructField("c1", ByteType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 2 > upperBound 2"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true
    )
  }

  test("Test BIT(1) Lower bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testBit1LowerBound(false)
  }

  test("Test BIT(1) as key Lower bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testBit1LowerBound(true)
  }

  private def testBit1LowerBound(testKey: Boolean): Unit = {

    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 BIT(1) primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 BIT(1))"
      )
    }

    val row = Row((-1).toByte)
    val schema = StructType(
      List(
        StructField("c1", ByteType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value -1 < lowerBound 0"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true
    )
  }

  test("Test BIT(4) Upper bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testBit4UpperBound(false)
  }

  test("Test BIT(4) as key Upper bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testBit4UpperBound(true)
  }

  private def testBit4UpperBound(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 BIT(4) primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 BIT(4))"
      )
    }

    val row = Row(16.toByte)
    val schema = StructType(
      List(
        StructField("c1", ByteType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 16 > upperBound 16"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true
    )
  }

  test("Test BIT(4) Lower bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testBit4LowerBound(false)
  }

  test("Test BIT(4) as key Lower bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testBit4LowerBound(true)
  }

  private def testBit4LowerBound(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 BIT(4) primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 BIT(4))"
      )
    }

    val row = Row((-1).toByte)
    val schema = StructType(
      List(
        StructField("c1", ByteType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value -1 < lowerBound 0"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true
    )
  }

  test("Test BIT(8) Upper bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testBit8UpperBound(false)
  }

  test("Test BIT(8) as key Upper bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testBit8UpperBound(true)
  }

  private def testBit8UpperBound(testKey: Boolean): Unit = {

    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 BIT(8) primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 BIT(8))"
      )
    }

    val row = Row(256L)
    val schema = StructType(
      List(
        StructField("c1", LongType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 256 > upperBound 256"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true
    )
  }

  test("Test BIT(8) Lower bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testBit8LowerBound(false)
  }

  test("Test BIT(8) as key Lower bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testBit8LowerBound(true)
  }

  private def testBit8LowerBound(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 BIT(8) primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 BIT(8))"
      )
    }

    val row = Row(-1L)
    val schema = StructType(
      List(
        StructField("c1", LongType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value -1 < lowerBound 0"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg,
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
