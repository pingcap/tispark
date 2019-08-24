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
class SignedOverflowSuite extends BaseDataSourceTest {

  test("Test TINYINT Upper bound Overflow") {
    testTinyIntUpperBound(testKey = false, "tinyint_upper_bound_overflow")
  }

  test("Test TINYINT as key Upper bound Overflow") {
    testTinyIntUpperBound(testKey = true, "key_tinyint_upper_bound_overflow")
  }

  private def testTinyIntUpperBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 TINYINT primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 TINYINT)",
        table
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
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test TINYINT Lower bound Overflow") {
    testTinyIntLowerBound(testKey = false, "tinyint_lower_bound_overflow")
  }

  test("Test TINYINT as key Lower bound Overflow") {
    testTinyIntLowerBound(testKey = true, "key_tinyint_lower_bound_overflow")
  }

  private def testTinyIntLowerBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 TINYINT primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 TINYINT)",
        table
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
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test SMALLINT Upper bound Overflow") {
    testSmallIntUpperBound(testKey = false, "smallint_upper_bound_overflow")
  }

  test("Test SMALLINT as key Upper bound Overflow") {
    testSmallIntUpperBound(testKey = true, "key_smallint_upper_bound_overflow")
  }

  private def testSmallIntUpperBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 SMALLINT primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 SMALLINT)",
        table
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
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test SMALLINT Lower bound Overflow") {
    testSmallIntLowerBound(testKey = false, "small_int_lower_bound_overflow")
  }

  test("Test SMALLINT as key Lower bound Overflow") {
    testSmallIntLowerBound(testKey = true, "key_small_int_lower_bound_overflow")
  }

  private def testSmallIntLowerBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 SMALLINT primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 SMALLINT)",
        table
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
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test MEDIUMINT Upper bound Overflow") {
    testMediumIntUpperBound(testKey = false, "mediumint_upper_bound_overflow")
  }

  test("Test MEDIUMINT as key Upper bound Overflow") {
    testMediumIntUpperBound(testKey = true, "key_mediumint_upper_bound_overflow")
  }

  private def testMediumIntUpperBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 MEDIUMINT primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 MEDIUMINT)",
        table
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
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test MEDIUMINT Lower bound Overflow") {
    testMediumIntLowerBound(testKey = false, "mediumint_lower_bound_overflow")
  }

  test("Test MEDIUMINT as key Lower bound Overflow") {
    testMediumIntLowerBound(testKey = true, "key_mediumint_lower_bound_overflow")
  }

  private def testMediumIntLowerBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 MEDIUMINT primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 MEDIUMINT)",
        table
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
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test INT Upper bound Overflow") {
    testIntUpperBound(testKey = false, "int_upper_bound_overflow")
  }

  test("Test INT as key Upper bound Overflow") {
    testIntUpperBound(testKey = true, "key_int_upper_bound_overflow")
  }

  private def testIntUpperBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 INT primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 INT)",
        table
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
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test INT Lower bound Overflow") {
    testIntLowerBound(testKey = false, "int_lower_bound_overflow")
  }

  test("Test INT as key Lower bound Overflow") {
    testIntLowerBound(testKey = true, "key_int_lower_bound_overflow")
  }

  private def testIntLowerBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 INT primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 INT)",
        table
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
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test BIGINT Upper bound Overflow") {
    testBigIntUpperBound(testKey = false, "big_int_upper_bound_overflow")
  }

  test("Test BIGINT as key Upper bound Overflow") {
    testBigIntUpperBound(testKey = true, "key_big_int_upper_bound_overflow")
  }

  private def testBigIntUpperBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 BIGINT primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 BIGINT)",
        table
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
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test BIGINT Lower bound Overflow") {
    testBigIntLowerBound(testKey = false, "big_int_lower_bound_overflow")
  }

  test("Test BIGINT as key Lower bound Overflow") {
    testBigIntLowerBound(testKey = true, "key_big_int_lower_bound_overflow")
  }

  private def testBigIntLowerBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 BIGINT primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 BIGINT)",
        table
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
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test BOOLEAN Upper bound Overflow") {
    testBooleanUpperBound(testKey = false, "boolean_upper_bound_overflow")
  }

  test("Test BOOLEAN as key Upper bound Overflow") {
    testBooleanUpperBound(testKey = true, "key_boolean_upper_bound_overflow")
  }

  private def testBooleanUpperBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 BOOLEAN primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 BOOLEAN)",
        table
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
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test BOOLEAN Lower bound Overflow") {
    testBooleanLowerBound(testKey = false, "boolean_lower_bound_overflow")
  }

  test("Test BOOLEAN as key Lower bound Overflow") {
    testBooleanLowerBound(testKey = true, "key_boolean_lower_bound_overflow")
  }

  private def testBooleanLowerBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 BOOLEAN primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 BOOLEAN)",
        table
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
      table,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }
}
