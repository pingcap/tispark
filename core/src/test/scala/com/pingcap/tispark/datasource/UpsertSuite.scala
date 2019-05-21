package com.pingcap.tispark.datasource

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class UpsertSuite extends BaseDataSourceSuite("test_datasource_upsert") {
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2, "TiDB")
  private val row3 = Row(3, "Spark")
  private val row4 = Row(4, null)
  private val row5 = Row(5, "Duplicate")

  private val row2_v2 = Row(2, "TiSpark")
  private val row3_v2 = Row(3, "TiSpark")
  private val row4_v2 = Row(4, "TiSpark")
  private val row5_v2 = Row(5, "TiSpark")

  private val schema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("s", StringType)
    )
  )

  test("Test upsert to table without primary key") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(null, 'Hello')"
    )

    // insert row2 row3
    batchWrite(List(row2, row3), schema)
    testSelect(Seq(row1, row2, row3))

    // insert row2 row4
    batchWrite(List(row2, row4), schema)
    testSelect(Seq(row1, row2, row2, row3, row4))

    // insert row5 row5
    batchWrite(List(row5, row5), schema)
    testSelect(Seq(row1, row2, row2, row3, row4, row5, row5))

    // insert row3_v2
    batchWrite(List(row3_v2), schema)
    testSelect(Seq(row1, row2, row2, row3, row3_v2, row4, row5, row5))
  }

  test("Test upsert to table with primary key (primary key is handle)") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i int primary key, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(2, 'TiDB')"
    )

    // insert row3 row4
    batchWrite(List(row3, row4), schema)
    testSelect(Seq(row2, row3, row4))

    // insert row2_v2 row5
    batchWrite(List(row2_v2, row5), schema)
    testSelect(Seq(row2_v2, row3, row4, row5))

    // insert row3_v2 row4_v2 row5_v2
    batchWrite(List(row3_v2, row4_v2, row5_v2), schema)
    testSelect(Seq(row2_v2, row3_v2, row4_v2, row5_v2))
  }

  test("Test upsert to table with primary key (auto increase case 1)") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i int primary key AUTO_INCREMENT, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(2, 'TiDB')"
    )

    // insert row3 row4
    batchWrite(List(row3, row4), schema)
    testSelect(Seq(row2, row3, row4))

    // insert row2_v2 row5
    batchWrite(List(row2_v2, row5), schema)
    testSelect(Seq(row2_v2, row3, row4, row5))

    // insert row3_v2 row4_v2 row5_v2
    batchWrite(List(row3_v2, row4_v2, row5_v2), schema)
    testSelect(Seq(row2_v2, row3_v2, row4_v2, row5_v2))
  }

  // TODO: support auto increment
  test("Test upsert to table with primary key (auto increase case 2)") {
    val rowWithoutPK2 = Row("TiDB")
    val rowWithoutPK3 = Row("Spark")
    val rowWithoutPK4 = Row(null)
    val rowWithoutPK5 = Row("Duplicate")

    val row1 = Row(1, "Hello")
    val row2 = Row(2, "TiDB")
    val row3 = Row(3, "Spark")
    val row4 = Row(4, null)
    val row5 = Row(5, "Duplicate")

    dropTable()
    jdbcUpdate(s"create table $dbtable(i int primary key AUTO_INCREMENT, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable(s) values('Hello')"
    )

    val schema2 = StructType(
      List(
        StructField("s", StringType)
      )
    )
    // insert row2 row3
    batchWrite(List(rowWithoutPK2, rowWithoutPK3), schema2)
    testSelect(Seq(row1, row2, row3))

    // insert row4 row5
    batchWrite(List(rowWithoutPK4, rowWithoutPK5), schema)
    testSelect(Seq(row1, row2, row3, row4, row5))
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
