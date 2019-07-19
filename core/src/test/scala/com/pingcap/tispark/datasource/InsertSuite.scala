package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

class InsertSuite extends BaseDataSourceTest("test_datasource_insert") {
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
    dropTable()
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(null, 'Hello')"
    )

    var data = List(row1)
    // insert 2 rows
    var insert = generateData(2, 2)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)

    // insert duplicate row
    insert = generateData(2, 2)
    data = data ::: insert
    // sort the data
    data = data.sortWith(compareRow)
    tidbWrite(insert, schema)
    testTiDBSelect(data)

    // insert ~100 rows
    insert = generateData(5, 95)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)

    // insert ~1000 rows
    insert = generateData(101, 900)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)
  }

  test("Test insert to table with primary key (primary key is handle)") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i int primary key, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(2, 'TiDB')"
    )

    var data = List(Row(2, "TiDB"))
    // insert 2 rows
    var insert = generateData(3, 2)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)

    // insert duplicate row
    insert = generateData(4, 2)
    intercept[TiBatchWriteException] {
      tidbWrite(insert, schema)
    }

    // insert ~100 rows
    insert = generateData(5, 95)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)

    // insert ~1000 rows
    insert = generateData(101, 900)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)
  }

  test("Test insert to table with primary key with tiny int") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i tinyint primary key, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(2, 'TiDB')"
    )

    var data = List(Row(2, "TiDB"))
    // insert 2 rows
    val insert = generateData(3, 100)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)
  }

  test("Test insert to table with primary key with small int") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i smallint primary key, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(2, 'TiDB')"
    )

    var data = List(Row(2, "TiDB"))
    // insert 2 rows
    val insert = generateData(3, 100)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)
  }

  test("Test insert to table with primary key with medium int") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i mediumint primary key, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(2, 'TiDB')"
    )

    var data = List(Row(2, "TiDB"))
    // insert 2 rows
    val insert = generateData(3, 100)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)
  }

  test("Test insert to table with primary key (auto increase case 1)") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i int primary key AUTO_INCREMENT, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(2, 'TiDB')"
    )

    var data = List(Row(2, "TiDB"))
    // insert 2 rows
    var insert = generateData(3, 2)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)

    // when provide auto id column value but say not provide them in options
    // an exception will be thrown.
    // duplicate pk
    intercept[TiBatchWriteException] {
      tidbWrite(List(row2_v2, row5), schema)
    }

    // when not provide auto id but say provide them in options
    // and exception will be thrown.
    // null pk
    intercept[TiBatchWriteException] {
      tidbWrite(List(Row(null, "abc")), schema)
    }

    //insert ~100 rows
    insert = generateData(5, 95)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)

    //insert ~1000 rows
    insert = generateData(101, 900)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)
  }

  test("Test insert to table with primary key (auto increase case 2)") {

    dropTable()
    jdbcUpdate(s"create table $dbtable(i int primary key AUTO_INCREMENT, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable(s) values('Hello')"
    )

    val withOutIDSchema = StructType(
      List(
        StructField("s", StringType)
      )
    )

    var data = List(Row("Hello"))

    // insert 2 rows
    var insert = generateData(30000, 2, true)
    data = data ::: insert
    tidbWrite(insert, withOutIDSchema)
    testTiDBSelect(data, "i", "s")

    // insert ~100 rows
    insert = generateData(30002, 98, true)
    data = data ::: insert
    tidbWrite(insert, withOutIDSchema)
    testTiDBSelect(data, "i", "s")

    // insert ~1000 rows
    insert = generateData(30100, 900, true)
    data = data ::: insert
    tidbWrite(insert, withOutIDSchema)
    testTiDBSelect(data, "i", "s")
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
