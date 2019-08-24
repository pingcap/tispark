package com.pingcap.tispark.datasource

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class EdgeConditionSuite extends BaseDataSourceTest {
  private val TEST_LARGE_DATA_SIZE = 102400

  private val TEST_LARGE_COLUMN_SIZE = 512

  override def beforeAll(): Unit =
    super.beforeAll()

  test("Write to table with one column (primary key long type)") {
    val table = "table_with_on_col_pk_is_long"
    val row1 = Row(1L)
    val row2 = Row(2L)
    val row3 = Row(3L)
    val row4 = Row(4L)

    val schema = StructType(
      List(
        StructField("i", LongType)
      )
    )

    dropTable(table)

    createTable(
      "create table `%s`.`%s`(i int, primary key (i))",
      table
    )
    jdbcUpdate(
      "insert into `%s`.`%s` values(1)",
      table
    )
    tidbWriteWithTable(List(row2, row3, row4), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row3, row4), tableName = table)
  }

  test("Write to table with one column (primary key int type)") {
    val table = "table_with_on_col_pk_is_int"
    val row1 = Row(1)
    val row2 = Row(2)
    val row3 = Row(3)
    val row4 = Row(4)

    val schema = StructType(
      List(
        StructField("i", IntegerType)
      )
    )

    dropTable(table)

    createTable(
      "create table `%s`.`%s`(i int, primary key (i))",
      table
    )
    jdbcUpdate(
      "insert into `%s`.`%s` values(1)",
      table
    )
    tidbWriteWithTable(List(row2, row3, row4), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row3, row4), tableName = table)
  }

  test("Write to table with one column (primary key + auto increase)") {
    val table = "table_with_on_col_pk_and_auto"
    val row1 = Row(1L)
    val row2 = Row(2L)
    val row3 = Row(3L)
    val row4 = Row(4L)

    val schema = StructType(
      List(
        StructField("i", LongType)
      )
    )

    dropTable(table)

    createTable(
      "create table `%s`.`%s`(i int NOT NULL AUTO_INCREMENT, primary key (i))",
      table
    )
    jdbcUpdate(
      "insert into `%s`.`%s` values(1)",
      table
    )
    tidbWriteWithTable(List(row2, row3, row4), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row3, row4), tableName = table)
  }

  test("Write to table with one column (no primary key)") {
    val table = "table_with_on_col_no_pk"
    val row1 = Row(null)
    val row2 = Row("Hello")
    val row3 = Row("Spark")
    val row4 = Row("TiDB")

    val schema = StructType(
      List(
        StructField("i", StringType)
      )
    )

    dropTable(table)

    createTable(
      "create table `%s`.`%s`(i varchar(128))",
      table
    )
    jdbcUpdate(
      "insert into `%s`.`%s` values('Hello')",
      table
    )
    tidbWriteWithTable(List(row1, row3, row4), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row3, row4), tableName = table)
  }

  test("Write to table with many columns") {
    val table = "table_with_many_cols"
    val types = ("int", LongType) :: ("varchar(128)", StringType) :: Nil
    val data1 = 1L :: "TiDB" :: Nil
    val data2 = 2L :: "Spark" :: Nil

    val row1 = Row.fromSeq(
      (0 until TEST_LARGE_COLUMN_SIZE).map { i =>
        data1(i % data1.size)
      }
    )

    val row2 = Row.fromSeq(
      (0 until TEST_LARGE_COLUMN_SIZE).map { i =>
        data2(i % data2.size)
      }
    )

    val schema = StructType(
      (0 until TEST_LARGE_COLUMN_SIZE)
        .map { i =>
          StructField(s"c$i", types(i % types.size)._2)
        }
    )

    val createTableSchemaStr = (0 until TEST_LARGE_COLUMN_SIZE)
      .map { i =>
        s"c$i ${types(i % types.size)._1}"
      }
      .mkString(", ")

    dropTable(table)

    createTable(
      s"create table `%s`.`%s`($createTableSchemaStr)",
      table
    )

    tidbWriteWithTable(List(row1, row2), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2), "c0", tableName = table)
  }

  test("Write Empty data") {
    val table = "write_empty_data"
    val row1 = Row(1L)

    val schema = StructType(
      List(
        StructField("i", LongType)
      )
    )

    dropTable(table)

    createTable(
      "create table `%s`.`%s`(i int, primary key (i))",
      table
    )
    jdbcUpdate(
      "insert into `%s`.`%s` values(1)",
      table
    )
    tidbWriteWithTable(List(), schema, table)
    testTiDBSelectWithTable(Seq(row1), tableName = table)
  }

  test("Write large amount of data") {
    val table = "write_large_amount_data"
    var list: List[Row] = Nil
    for (i <- 0 until TEST_LARGE_DATA_SIZE) {
      list = Row(i.toLong) :: list
    }
    list = list.reverse

    val schema = StructType(
      List(
        StructField("i", LongType)
      )
    )

    dropTable(table)

    createTable(
      "create table `%s`.`%s`(i int, primary key (i))",
      table
    )
    tidbWriteWithTable(list, schema, table)
    testTiDBSelectWithTable(list, tableName = table)

    var list2: List[Row] = Nil
    for (i <- TEST_LARGE_DATA_SIZE until TEST_LARGE_DATA_SIZE * 2) {
      list2 = Row(i.toLong) :: list2
    }
    list2 = list2.reverse

    tidbWriteWithTable(list2, schema, table)
    testTiDBSelectWithTable(list ::: list2, tableName = table)
  }
}
