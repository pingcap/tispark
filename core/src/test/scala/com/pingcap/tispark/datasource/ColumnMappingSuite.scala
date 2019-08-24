package com.pingcap.tispark.datasource

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

class ColumnMappingSuite extends BaseDataSourceTest {

  val table = "test_datasource_insert_with_different_column_order"
  private val schema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("s", StringType),
      StructField("c", StringType)
    )
  )

  private def generateData(start: Int,
                           length: Int,
                           posMap: List[Int],
                           skipFirstCol: Boolean = false): (List[Row], List[Row]) = {
    val strings1 = Array("Hello", "TiDB", "Spark", null, "TiSpark")
    val strings2 = Array("TiDB", "Spark", null, "TiSpark", "Hello")
    val ret1 = ArrayBuffer[Row]()
    val ret2 = ArrayBuffer[Row]()
    for (x <- start until start + length) {
      if (skipFirstCol) {
        val r = Row(strings1(x % strings1.length), strings2(x % strings2.length))
        ret1 += Row(x, r.get(0), r.get(1))
        ret2 += Row(r.get(posMap(0)), r.get(posMap(1)))
      } else {
        val r = Row(x, strings1(x % strings1.length), strings2(x % strings2.length))
        ret1 += r
        ret2 += Row(r.get(posMap(0)), r.get(posMap(1)), r.get(posMap(2)))
      }
    }
    (ret1.toList, ret2.toList)
  }

  test("Test different column order with full schema") {
    dropTable(table)
    createTable(
      s"create table `%s`.`%s`(i int primary key auto_increment, s varchar(128), c varchar(128))",
      table
    )

    var posMap = List(1, 2, 0)
    var data = generateData(0, 10, posMap)

    var ans = data._1
    var writeSchema = StructType(
      List(
        schema.toList(posMap(0)),
        schema.toList(posMap(1)),
        schema.toList(posMap(2))
      )
    )
    tidbWriteWithTable(data._2, writeSchema, table)
    testTiDBSelectWithTable(ans, tableName = table)

    posMap = List(0, 2, 1)
    data = generateData(10, 10, posMap)
    ans = ans ::: data._1
    writeSchema = StructType(
      List(
        schema.toList(posMap(0)),
        schema.toList(posMap(1)),
        schema.toList(posMap(2))
      )
    )
    tidbWriteWithTable(data._2, writeSchema, table)
    testTiDBSelectWithTable(ans, tableName = table)

    posMap = List(1, 2, 0)
    data = generateData(20, 10, posMap)
    ans = ans ::: data._1
    writeSchema = StructType(
      List(
        schema.toList(posMap(0)),
        schema.toList(posMap(1)),
        schema.toList(posMap(2))
      )
    )
    tidbWriteWithTable(data._2, writeSchema, table)
    testTiDBSelectWithTable(ans, tableName = table)

    posMap = List(2, 1, 0)
    data = generateData(30, 10, posMap)
    ans = ans ::: data._1
    writeSchema = StructType(
      List(
        schema.toList(posMap(0)),
        schema.toList(posMap(1)),
        schema.toList(posMap(2))
      )
    )
    tidbWriteWithTable(data._2, writeSchema, table)
    testTiDBSelectWithTable(ans, tableName = table)
  }

  test("Test different column order without auto increment column") {
    dropTable(table)
    createTable(
      s"create table `%s`.`%s`(i int primary key auto_increment, s varchar(128), c varchar(128))",
      table
    )

    // insert 2 rows
    //val (ref, insert) = generateData(1,10, List(2,0,1))
    var posMap = List(1, 0)
    var data = generateData(0, 10, posMap, skipFirstCol = true)
    var ans = data._1

    var writeSchema = StructType(
      List(
        schema.toList(posMap(0) + 1),
        schema.toList(posMap(1) + 1)
      )
    )
    tidbWriteWithTable(data._2, writeSchema, table)
    testTiDBSelectWithTable(ans, tableName = table)

    posMap = List(0, 1)
    data = generateData(10, 10, posMap, skipFirstCol = true)
    ans = ans ::: data._1
    writeSchema = StructType(
      List(
        schema.toList(posMap(0) + 1),
        schema.toList(posMap(1) + 1)
      )
    )
    tidbWriteWithTable(data._2, writeSchema, table)
    testTiDBSelectWithTable(ans, tableName = table)
  }

  override def afterAll(): Unit =
    try {
      dropTable(table)
    } finally {
      super.afterAll()
    }
}
