package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * SINGED type include:
 * 1. TINYINT SINGED
 * 2. SMALLINT SINGED
 * 3. MEDIUMINT SINGED
 * 4. INT SINGED
 * 5. BIGINT SINGED
 * 6. BOOLEAN
 */
class SignedOverflowSuite extends BaseDataSourceTest("test_data_type_signed_overflow") {

  test("Test TINYINT Upper bound Overflow") {
    testTinyIntUpperBound(false)
  }

  test("Test TINYINT as key Upper bound Overflow") {
    testTinyIntUpperBound(true)
  }

  private def testTinyIntUpperBound(testKey : Boolean): Unit = {
    dropTable()
    if(testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 TINYINT primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 TINYINT)"
      )
    }

    val row = Row(128)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 128 > upperBound 127"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test TINYINT Lower bound Overflow") {
    testTinyIntLowerBound(false)
  }

  test("Test TINYINT as key Lower bound Overflow") {
    testTinyIntLowerBound(true)
  }

  private def testTinyIntLowerBound(testKey : Boolean): Unit = {
    dropTable()
    if(testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 TINYINT primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 TINYINT)"
      )
    }

    val row = Row(-129)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value -129 < lowerBound -128"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test SMALLINT Upper bound Overflow") {
    testSmallIntUpperBound(false)
  }

  test("Test SMALLINT as key Upper bound Overflow") {
    testSmallIntUpperBound(true)
  }

  private def testSmallIntUpperBound(testKey : Boolean): Unit = {
    dropTable()
    if(testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 SMALLINT primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 SMALLINT)"
      )
    }

    val row = Row(32768)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 32768 > upperBound 32767"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test SMALLINT Lower bound Overflow") {
    testSmallIntLowerBound(false)
  }

  test("Test SMALLINT as key Lower bound Overflow") {
    testSmallIntLowerBound(true)
  }

  private def testSmallIntLowerBound(testKey : Boolean): Unit = {
    dropTable()
    if(testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 SMALLINT primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 SMALLINT)"
      )
    }

    val row = Row(-32769)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value -32769 < lowerBound -32768"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test MEDIUMINT Upper bound Overflow") {
    testMediumIntUpperBound(false)
  }

  test("Test MEDIUMINT as key Upper bound Overflow") {
    testMediumIntUpperBound(true)
  }

  private def testMediumIntUpperBound(testKey : Boolean): Unit = {
    dropTable()
    if(testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 MEDIUMINT primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 MEDIUMINT)"
      )
    }

    val row = Row(8388608)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 8388608 > upperBound 8388607"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test MEDIUMINT Lower bound Overflow") {
    testMediumIntLowerBound(false)
  }

  test("Test MEDIUMINT as key Lower bound Overflow") {
    testMediumIntLowerBound(true)
  }

  private def testMediumIntLowerBound(testKey : Boolean): Unit = {
    dropTable()
    if(testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 MEDIUMINT primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 MEDIUMINT)"
      )
    }

    val row = Row(-8388609)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value -8388609 < lowerBound -8388608"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test INT Upper bound Overflow") {
    testIntUpperBound(false)
  }

  test("Test INT as key Upper bound Overflow") {
    testIntUpperBound(true)
  }

  private def testIntUpperBound(testKey : Boolean): Unit = {
    dropTable()
    if(testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 INT primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 INT)"
      )
    }

    val row = Row(2147483648L)
    val schema = StructType(
      List(
        StructField("c1", LongType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 2147483648 > upperBound 2147483647"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test INT Lower bound Overflow") {
    testIntLowerBound(false)
  }

  test("Test INT as key Lower bound Overflow") {
    testIntLowerBound(true)
  }

  private def testIntLowerBound(testKey : Boolean): Unit = {
    dropTable()
    if(testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 INT primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 INT)"
      )
    }

    val row = Row(-2147483649L)
    val schema = StructType(
      List(
        StructField("c1", LongType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value -2147483649 < lowerBound -2147483648"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test BIGINT Upper bound Overflow") {
    testBigIntUpperBound(false)
  }

  test("Test BIGINT as key Upper bound Overflow") {
    testBigIntUpperBound(true)
  }

  private def testBigIntUpperBound(testKey : Boolean): Unit = {
    dropTable()
    if(testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 BIGINT primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 BIGINT)"
      )
    }

    val row = Row("9223372036854775808")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[java.lang.NumberFormatException]
    val tidbErrorMsg = "For input string: \"9223372036854775808\""

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test BIGINT Lower bound Overflow") {
    testBigIntLowerBound(false)
  }

  test("Test BIGINT as key Lower bound Overflow") {
    testBigIntLowerBound(true)
  }

  private def testBigIntLowerBound(testKey : Boolean): Unit = {
    dropTable()
    if(testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 BIGINT primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 BIGINT)"
      )
    }

    val row = Row("-9223372036854775809")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[java.lang.NumberFormatException]
    val tidbErrorMsg = "For input string: \"-9223372036854775809\""

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test BOOLEAN Upper bound Overflow") {
    testBooleanUpperBound(false)
  }

  test("Test BOOLEAN as key Upper bound Overflow") {
    testBooleanUpperBound(true)
  }

  private def testBooleanUpperBound(testKey : Boolean): Unit = {
    dropTable()
    if(testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 BOOLEAN primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 BOOLEAN)"
      )
    }

    val row = Row(128)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 128 > upperBound 127"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test BOOLEAN Lower bound Overflow") {
    testBooleanLowerBound(false)
  }

  test("Test BOOLEAN as key Lower bound Overflow") {
    testBooleanLowerBound(true)
  }

  private def testBooleanLowerBound(testKey : Boolean): Unit = {
    dropTable()
    if(testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 BOOLEAN primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 BOOLEAN)"
      )
    }

    val row = Row(-129)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value -129 < lowerBound -128"

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
