package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, _}

/**
 * SRTING type include:
 * 1. CHAR
 * 2. VARCHAR
 * 3. TINYTEXT
 * 4. TEXT
 * 5. MEDIUMTEXT
 * 6. LONGTEXT
 */
class StringOverflowSuite extends BaseDataSourceTest {

  test("Test CHAR Overflow") {
    testCharOverflow(testKey = false, "char_overflow")
  }

  test("Test CHAR as key Overflow") {
    testCharOverflow(testKey = true, "key_char_overflow")
  }

  private def testCharOverflow(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 CHAR(8) primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 CHAR(8))",
        table
      )
    }

    val row = Row("123456789")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Data too long for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 123456789 length > max length 8"

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

  test("Test VARCHAR Overflow") {
    testVarcharOverflow(testKey = false, "varchar_overflow")
  }

  test("Test VARCHAR as key Overflow") {
    testVarcharOverflow(testKey = true, "key_varchar_overflow")
  }

  private def testVarcharOverflow(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 VARCHAR(8) primary key)",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 VARCHAR(8))",
        table
      )
    }

    val row = Row("123456789")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Data too long for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 123456789 length > max length 8"

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

  test("Test TINYTEXT Overflow") {
    testTinyTextOverflow(testKey = false, "tiny_text_overflow")
  }

  test("Test TINYTEXT as key Overflow") {
    testTinyTextOverflow(testKey = true, "key_tiny_text_overflow")
  }

  private def testTinyTextOverflow(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 TINYTEXT, primary key (c1(4)))",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 TINYTEXT)",
        table
      )
    }

    val base = "0123456789"
    var str = ""
    for (i <- 1 to 30) {
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
    val tidbErrorMsg = s"value $str length > max length 255"

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

  test("Test TEXT Overflow") {
    testTextOverflow(testKey = false, "text_overflow")
  }

  test("Test TEXT as key Overflow") {
    testTextOverflow(testKey = true, "key_text_overflow")
  }

  private def testTextOverflow(testKey: Boolean, table: String): Unit = {
    dropTable(table)
    if (testKey) {
      createTable(
        "create table `%s`.`%s`(c1 TEXT(8), primary key (c1(4)))",
        table
      )
    } else {
      createTable(
        "create table `%s`.`%s`(c1 TEXT(8))",
        table
      )
    }

    val row = Row("123456789")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Data too long for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 123456789 length > max length 8"

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
