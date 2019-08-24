package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

class InsertSuite extends BaseDataSourceTest {
  private val row1 = Row(null, "Hello")
  private val row5 = Row(5, "Duplicate")

  private val row2_v2 = Row(2, "TiSpark")

  private val schema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("s", StringType)
    )
  )

  private def compareRow(r1: Row, r2: Row): Boolean = {
    r1.getAs[Int](0) < r2.getAs[Int](0)
  }

  private def generateData(start: Int, length: Int, skipFirstCol: Boolean = false): List[Row] = {
    val strings = Array("Hello", "TiDB", "Spark", null, "TiSpark")
    val ret = ArrayBuffer[Row]()
    for (x <- start until start + length) {
      if (skipFirstCol) {
        ret += Row(strings(x % strings.length))
      } else {
        ret += Row(x, strings(x % strings.length))
      }
    }
    ret.toList
  }

  test("Test insert to table without primary key") {
    val table = "pk_wo_pk"
    dropTable(table)
    createTable(s"create table `%s`.`%s`(i int, s varchar(128))", table)
    jdbcUpdate(
      s"insert into `%s`.`%s` values(null, 'Hello')",
      table
    )

    var data = List(row1)
    // insert 2 rows
    var insert = generateData(2, 2)
    data = data ::: insert
    tidbWriteWithTable(insert, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    // insert duplicate row
    insert = generateData(2, 2)
    data = data ::: insert
    // sort the data
    data = data.sortWith(compareRow)
    tidbWriteWithTable(insert, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    // insert ~100 rows
    insert = generateData(5, 95)
    data = data ::: insert
    tidbWriteWithTable(insert, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    // insert ~1000 rows
    insert = generateData(101, 900)
    data = data ::: insert
    tidbWriteWithTable(insert, schema, table)
    testTiDBSelectWithTable(data, tableName = table)
  }

  test("Test insert to table with primary key (primary key is handle)") {
    val table = "tbl_pk_is_handle"
    dropTable(table)
    createTable(s"create table `%s`.`%s`(i int primary key, s varchar(128))", table)
    jdbcUpdate(
      s"insert into `%s`.`%s` values(2, 'TiDB')",
      table
    )

    var data = List(Row(2, "TiDB"))
    // insert 2 rows
    var insert = generateData(3, 2)
    data = data ::: insert
    tidbWriteWithTable(insert, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    // insert duplicate row
    insert = generateData(4, 2)
    intercept[TiBatchWriteException] {
      tidbWriteWithTable(insert, schema, table)
    }

    // insert ~100 rows
    insert = generateData(5, 95)
    data = data ::: insert
    tidbWriteWithTable(insert, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    // insert ~1000 rows
    insert = generateData(101, 900)
    data = data ::: insert
    tidbWriteWithTable(insert, schema, table)
    testTiDBSelectWithTable(data, tableName = table)
  }

  test("Test insert to table with primary key with tiny int") {
    val table = "tbl_pk_is_tiny_int"
    dropTable(table)
    createTable(s"create table `%s`.`%s`(i tinyint primary key, s varchar(128))", table)
    jdbcUpdate(
      s"insert into `%s`.`%s` values(2, 'TiDB')",
      table
    )

    var data = List(Row(2, "TiDB"))
    // insert 2 rows
    val insert = generateData(3, 100)
    data = data ::: insert
    tidbWriteWithTable(insert, schema, table)
    testTiDBSelectWithTable(data, tableName = table)
  }

  test("Test insert to table with primary key with small int") {
    val table = "tbl_pk_is_small_int"
    dropTable(table)
    createTable(s"create table `%s`.`%s`(i smallint primary key, s varchar(128))", table)
    jdbcUpdate(
      s"insert into `%s`.`%s` values(2, 'TiDB')",
      table
    )

    var data = List(Row(2, "TiDB"))
    // insert 2 rows
    val insert = generateData(3, 100)
    data = data ::: insert
    tidbWriteWithTable(insert, schema, table)
    testTiDBSelectWithTable(data, tableName = table)
  }

  test("Test insert to table with primary key with medium int") {
    val table = "tbl_pk_is_medium_int"
    dropTable(table)
    createTable(s"create table `%s`.`%s`(i mediumint primary key, s varchar(128))", table)
    jdbcUpdate(
      s"insert into `%s`.`%s` values(2, 'TiDB')",
      table
    )

    var data = List(Row(2, "TiDB"))
    // insert 2 rows
    val insert = generateData(3, 100)
    data = data ::: insert
    tidbWriteWithTable(insert, schema, table)
    testTiDBSelectWithTable(data, tableName = table)
  }

  test("Test insert to table with primary key (auto increase case 1)") {
    val table = "tbl_pk_is_handle_auto"
    dropTable(table)
    createTable("create table `%s`.`%s`(i int primary key AUTO_INCREMENT, s varchar(128))", table)
    jdbcUpdate(
      "insert into `%s`.`%s` values(2, 'TiDB')",
      table
    )

    var data = List(Row(2, "TiDB"))
    // insert 2 rows
    var insert = generateData(3, 2)
    data = data ::: insert
    tidbWriteWithTable(insert, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    // when provide auto id column value but say not provide them in options
    // an exception will be thrown.
    // duplicate pk
    intercept[TiBatchWriteException] {
      tidbWriteWithTable(List(row2_v2, row5), schema, table)
    }

    // when not provide auto id but say provide them in options
    // and exception will be thrown.
    // null pk
    intercept[TiBatchWriteException] {
      tidbWriteWithTable(List(Row(null, "abc")), schema, table)
    }

    //insert ~100 rows
    insert = generateData(5, 95)
    data = data ::: insert
    tidbWriteWithTable(insert, schema, table)
    testTiDBSelectWithTable(data, tableName = table)

    //insert ~1000 rows
    insert = generateData(101, 900)
    data = data ::: insert
    tidbWriteWithTable(insert, schema, table)
    testTiDBSelectWithTable(data, tableName = table)
  }

  test("Test insert to table with primary key (auto increase case 2)") {
    val table = "tbl_pk_is_handle_auto_case_2"
    dropTable(table)
    createTable(s"create table `%s`.`%s`(i int primary key AUTO_INCREMENT, s varchar(128))", table)
    jdbcUpdate(
      s"insert into `%s`.`%s`(s) values('Hello')",
      table
    )

    val withOutIDSchema = StructType(
      List(
        StructField("s", StringType)
      )
    )

    var data = List(Row("Hello"))

    // insert 2 rows
    var insert = generateData(30000, 2, skipFirstCol = true)
    data = data ::: insert
    tidbWriteWithTable(insert, withOutIDSchema, table)
    testTiDBSelectWithTable(data, "i", "s", tableName = table)

    // insert ~100 rows
    insert = generateData(30002, 98, skipFirstCol = true)
    data = data ::: insert
    tidbWriteWithTable(insert, withOutIDSchema, table)
    testTiDBSelectWithTable(data, "i", "s", tableName = table)

    // insert ~1000 rows
    insert = generateData(30100, 900, skipFirstCol = true)
    data = data ::: insert
    tidbWriteWithTable(insert, withOutIDSchema, table)
    testTiDBSelectWithTable(data, "i", "s", tableName = table)
  }
}
