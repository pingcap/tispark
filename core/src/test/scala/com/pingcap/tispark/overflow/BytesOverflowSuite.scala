package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * BYTES type include:
 * 1. BINARY
 * 2. VARBINARY
 * 3. TINYBLOB
 * 4. BLOB
 * 5. MEDIUMBLOB
 * 6. LONGBLOB
 */
class BytesOverflowSuite extends BaseDataSourceTest {

  test("Test BINARY Overflow") {
    testBinaryOverflow(testKey = false, "binary_overflow")
  }

  test("Test BINARY as key Overflow") {
    testBinaryOverflow(testKey = true, "key_binary_overflow")
  }

  private def testBinaryOverflow(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      jdbcUpdate(
        s"create table `$database`.`$table`(c1 BINARY(8), primary key (c1(4)))"
      )
    } else {
      jdbcUpdate(
        s"create table `$database`.`$table`(c1 BINARY(8))"
      )
    }

    val row = Row("0123456789")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Data too long for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "length 10 > max length 8"

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

  test("Test VARBINARY Overflow") {
    testVarbinaryOverflow(testKey = false, "var_overflow")
  }

  test("Test VARBINARY as key Overflow") {
    testVarbinaryOverflow(testKey = true, "key_var_overflow")
  }

  private def testVarbinaryOverflow(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      jdbcUpdate(
        s"create table `$database`.`$table`(c1 VARBINARY(8), primary key (c1(4)))"
      )
    } else {
      jdbcUpdate(
        s"create table `$database`.`$table`(c1 VARBINARY(8))"
      )
    }

    val row = Row("0123456789")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Data too long for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "length 10 > max length 8"

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

  test("Test TINYBLOB Overflow") {
    testTinyBlobOverflow(testKey = false, "tinyblob_overflow")
  }

  test("Test TINYBLOB as key Overflow") {
    testTinyBlobOverflow(testKey = true, "key_tingblob_overflow")
  }

  private def testTinyBlobOverflow(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      jdbcUpdate(
        s"create table `$database`.`$table`(c1 TINYBLOB, primary key (c1(4)))"
      )
    } else {
      jdbcUpdate(
        s"create table `$database`.`$table`(c1 TINYBLOB)"
      )
    }

    val base = "0123456789"
    var str = ""
    for (_ <- 1 to 30) {
      str = str + base
    }
    val row = Row(str)

    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Data too long for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "length 300 > max length 255"

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
