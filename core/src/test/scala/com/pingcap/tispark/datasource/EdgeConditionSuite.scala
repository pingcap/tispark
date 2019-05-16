package com.pingcap.tispark.datasource

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

class EdgeConditionSuite extends BaseDataSourceSuite("test_datasource_edge_condition") {

  private val TEST_LARGE_DATA_SIZE = 102400

  private val TEST_LARGE_COLUMN_SIZE = 512

  override def beforeAll(): Unit =
    super.beforeAll()

  test("Write to table with one column (primary key long type)") {
    val row1 = Row(1L)
    val row2 = Row(2L)
    val row3 = Row(3L)
    val row4 = Row(4L)

    val schema = StructType(
      List(
        StructField("i", LongType)
      )
    )

    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i int, primary key (i))"
    )
    jdbcUpdate(
      s"insert into $dbtable values(1)"
    )
    batchWrite(List(row2, row3, row4), schema)
    testSelect(Seq(row1, row2, row3, row4))
  }

  test("Write to table with one column (primary key int type)") {
    val row1 = Row(1)
    val row2 = Row(2)
    val row3 = Row(3)
    val row4 = Row(4)

    val schema = StructType(
      List(
        StructField("i", IntegerType)
      )
    )

    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i int, primary key (i))"
    )
    jdbcUpdate(
      s"insert into $dbtable values(1)"
    )
    batchWrite(List(row2, row3, row4), schema)
    testSelect(Seq(row1, row2, row3, row4))
  }

  test("Write to table with one column (primary key + auto increase)") {
    val row1 = Row(1L)
    val row2 = Row(2L)
    val row3 = Row(3L)
    val row4 = Row(4L)

    val schema = StructType(
      List(
        StructField("i", LongType)
      )
    )

    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i int NOT NULL AUTO_INCREMENT, primary key (i))"
    )
    jdbcUpdate(
      s"insert into $dbtable values(1)"
    )
    batchWrite(List(row2, row3, row4), schema)
    testSelect(Seq(row1, row2, row3, row4))
  }

  test("Write to table with one column (no primary key)") {
    val row1 = Row(null)
    val row2 = Row("Hello")
    val row3 = Row("Spark")
    val row4 = Row("TiDB")

    val schema = StructType(
      List(
        StructField("i", StringType)
      )
    )

    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i varchar(128))"
    )
    jdbcUpdate(
      s"insert into $dbtable values('Hello')"
    )
    batchWrite(List(row1, row3, row4), schema)
    testSelect(Seq(row1, row2, row3, row4))
  }

  test("Write to table with many columns") {
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

    dropTable()

    jdbcUpdate(
      s"create table $dbtable($createTableSchemaStr)"
    )

    batchWrite(List(row1, row2), schema)
    testSelect(Seq(row1, row2), "c0")
  }

  test("Write Empty data") {
    val row1 = Row(1L)

    val schema = StructType(
      List(
        StructField("i", LongType)
      )
    )

    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i int, primary key (i))"
    )
    jdbcUpdate(
      s"insert into $dbtable values(1)"
    )
    batchWrite(List(), schema)
    testSelect(Seq(row1))
  }

  test("Write large amount of data") {
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

    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i int, primary key (i))"
    )
    batchWrite(list, schema)
    testSelect(list)

    var list2: List[Row] = Nil
    for (i <- TEST_LARGE_DATA_SIZE until TEST_LARGE_DATA_SIZE * 2) {
      list2 = Row(i.toLong) :: list2
    }
    list2 = list2.reverse

    batchWrite(list2, schema)
    testSelect(list ::: list2)
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
