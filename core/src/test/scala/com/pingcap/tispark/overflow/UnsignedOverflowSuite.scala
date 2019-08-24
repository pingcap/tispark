package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * UNSIGNED type include:
 * 1. TINYINT UNSIGNED
 * 2. SMALLINT UNSIGNED
 * 3. MEDIUMINT UNSIGNED
 * 4. INT UNSIGNED
 * 5. BIGINT UNSIGNED
 */
class UnsignedOverflowSuite extends BaseDataSourceTest {

  test("Test TINYINT UNSIGNED Upper bound Overflow") {
    testTinyIntUnsignedUpperBound(testKey = false, "tiny_int_unsigned_upper_bound_overflow")
  }

  test("Test TINYINT UNSIGNED as key Upper bound Overflow") {
    testTinyIntUnsignedUpperBound(testKey = true, "key_tiny_int_unsigned_upper_bound_overflow")
  }

  private def testTinyIntUnsignedUpperBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 TINYINT UNSIGNED primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 TINYINT UNSIGNED)",
        table
      )
    }

    val row = Row(256)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 256 > upperBound 255"

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

  test("Test TINYINT UNSIGNED Lower bound Overflow") {
    testTinyIntUnsignedLowerBound(testKey = false, "tiny_int_unsigned_lower_bound_overflow")
  }

  test("Test TINYINT UNSIGNED as key Lower bound Overflow") {
    testTinyIntUnsignedLowerBound(testKey = true, "key_tiny_int_unsigned_lower_bound_overflow")
  }

  private def testTinyIntUnsignedLowerBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 TINYINT UNSIGNED primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 TINYINT UNSIGNED)",
        table
      )
    }

    val row = Row(-1)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
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
      tidbErrorMsg
    )
  }

  test("Test SMALLINT UNSIGNED Upper bound Overflow") {
    testSmallIntUnsignedUpperBound(testKey = false, "small_int_unsigned_upper_bound_overflow")
  }

  test("Test SMALLINT UNSIGNED as key Upper bound Overflow") {
    testSmallIntUnsignedUpperBound(testKey = true, "key_small_int_unsigned_upper_bound_overflow")
  }

  private def testSmallIntUnsignedUpperBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 SMALLINT UNSIGNED primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 SMALLINT UNSIGNED)",
        table
      )
    }

    val row = Row(65536)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 65536 > upperBound 65535"

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

  test("Test SMALLINT UNSIGNED Lower bound Overflow") {
    testSmallIntUnsignedLowerBound(testKey = false, "small_int_unsigned_lower_bound_overflow")
  }

  test("Test SMALLINT UNSIGNED as key Lower bound Overflow") {
    testSmallIntUnsignedLowerBound(testKey = true, "key_small_int_unsigned_lower_bound_overflow")
  }

  private def testSmallIntUnsignedLowerBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 SMALLINT UNSIGNED primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 SMALLINT UNSIGNED)",
        table
      )
    }

    val row = Row(-1)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
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
      tidbErrorMsg
    )
  }

  test("Test MEDIUMINT UNSIGNED Upper bound Overflow") {
    testMediumIntUnsignedUpperBound(testKey = false, "medium_int_unsigned_upper_bound_overflow")
  }

  test("Test MEDIUMINT UNSIGNED as key Upper bound Overflow") {
    testMediumIntUnsignedUpperBound(testKey = true, "key_medium_int_unsigned_upper_bound_overflow")
  }

  private def testMediumIntUnsignedUpperBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 MEDIUMINT UNSIGNED primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 MEDIUMINT UNSIGNED)",
        table
      )
    }

    val row = Row(16777216)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 16777216 > upperBound 16777215"

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

  test("Test MEDIUMINT UNSIGNED Lower bound Overflow") {
    testMediumIntUnsignedLowerBound(testKey = false, "medium_int_unsigned_lower_bound_overflow")
  }

  test("Test MEDIUMINT UNSIGNED as key Lower bound Overflow") {
    testMediumIntUnsignedLowerBound(testKey = true, "key_medium_int_unsigned_lower_bound_overflow")
  }

  private def testMediumIntUnsignedLowerBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 MEDIUMINT UNSIGNED primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 MEDIUMINT UNSIGNED)",
        table
      )
    }

    val row = Row(-1)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
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
      tidbErrorMsg
    )
  }

  test("Test INT UNSIGNED Upper bound Overflow") {
    testIntUnsignedUpperBound(testKey = false, "int_unsigned_upper_bound_overflow")
  }

  test("Test INT UNSIGNED as key Upper bound Overflow") {
    testIntUnsignedUpperBound(testKey = true, "key_int_unsigned_upper_bound_overflow")
  }

  private def testIntUnsignedUpperBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 INT UNSIGNED primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 INT UNSIGNED)",
        table
      )
    }

    val row = Row(4294967296L)
    val schema = StructType(
      List(
        StructField("c1", LongType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 4294967296 > upperBound 4294967295"

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

  test("Test INT UNSIGNED Lower bound Overflow") {
    testIntUnsignedLowerBound(testKey = false, "unsigned_lower_bound_overflow")
  }

  test("Test INT UNSIGNED as key Lower bound Overflow") {
    testIntUnsignedLowerBound(testKey = true, "key_unsigned_lower_bound_overflow")
  }

  private def testIntUnsignedLowerBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 INT UNSIGNED primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 INT UNSIGNED)",
        table
      )
    }

    val row = Row(-1)
    val schema = StructType(
      List(
        StructField("c1", LongType)
      )
    )
    val jdbcErrorClass = classOf[java.lang.RuntimeException]
    val jdbcErrorMsgStartWith =
      "Error while encoding: java.lang.RuntimeException: java.lang.Integer is not a valid external type for schema of bigint\nif (assertnotnull(input[0, org.apache.spark.sql.Row, true]).isNullAt) null else validateexternaltype(getexternalrowfield(assertnotnull(input[0, org.apache.spark.sql.Row, true]), 0, c1), LongType) AS c1"
    val tidbErrorClass = classOf[java.lang.RuntimeException]
    val tidbErrorMsgStartWith =
      "Error while encoding: java.lang.RuntimeException: java.lang.Integer is not a valid external type for schema of bigint\nif (assertnotnull(input[0, org.apache.spark.sql.Row, true]).isNullAt) null else validateexternaltype(getexternalrowfield(assertnotnull(input[0, org.apache.spark.sql.Row, true]), 0, c1), LongType) AS c1"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      table,
      jdbcErrorClass,
      jdbcErrorMsgStartWith,
      tidbErrorClass,
      tidbErrorMsgStartWith,
      msgStartWith = true
    )
  }

  test("Test BIGINT UNSIGNED Upper bound Overflow") {
    testBigIntUnsignedUpperBound(testKey = false, "big_int_unsigned_upper_bound_overflow")
  }

  test("Test BIGINT UNSIGNED as key Upper bound Overflow") {
    testBigIntUnsignedUpperBound(testKey = true, "key_big_int_unsigned_upper_bound_overflow")
  }

  private def testBigIntUnsignedUpperBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 BIGINT UNSIGNED primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 BIGINT UNSIGNED)",
        table
      )
    }

    val row = Row("18446744073709551616")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[java.lang.NumberFormatException]
    val tidbErrorMsg = "Too large for unsigned long: 18446744073709551616"

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

  test("Test BIGINT UNSIGNED Lower bound Overflow") {
    testBigIntUnsignedLowerBound(false, "big_int_unsigned_lower_bound_overflow")
  }

  test("Test BIGINT UNSIGNED as key Lower bound Overflow") {
    testBigIntUnsignedLowerBound(true, "big_int_unsigned_lower_bound_overflow")
  }

  private def testBigIntUnsignedLowerBound(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 BIGINT UNSIGNED primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 BIGINT UNSIGNED)",
        table
      )
    }

    val row = Row("-1")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[java.lang.NumberFormatException]
    val tidbErrorMsg = "-1"

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
