package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class AddingIndexSuite extends BaseDataSourceTest("adding_index") {
  private val row1 = Row(1, 1, "Hello")
  private val row2 = Row(2, 2, "TiDB")
  private val row3 = Row(3, 3, "Spark")
  private val row4 = Row(4, 4, "abde")
  private val row5 = Row(5, 5, "Duplicate")

  private val schema = StructType(
    List(
      StructField("pk", IntegerType),
      StructField("i", IntegerType),
      StructField("s", StringType)
    )
  )

  test("pk is not handle adding unique index") {
    dropTable()
    jdbcUpdate(
      s"create table $dbtable(pk int, i int, s varchar(128), unique index(i), primary key(s))"
    )
    jdbcUpdate(
      s"insert into $dbtable values(1, 1, 'Hello')"
    )
    // insert row2 row3
    tidbWrite(List(row2, row3), schema)
    testTiDBSelect(Seq(row1, row2, row3))

    intercept[TiBatchWriteException] {
      // insert row2 row4
      tidbWrite(List(row2, row4), schema)
    }
  }

  test("pk is handle adding unique index") {
    dropTable()
    jdbcUpdate(
      s"create table $dbtable(pk int, i int, s varchar(128), unique index(i), primary key(pk))"
    )
    jdbcUpdate(
      s"insert into $dbtable values(1, 1, 'Hello')"
    )
    // insert row2 row3
    tidbWrite(List(row2, row3), schema)
    testTiDBSelect(Seq(row1, row2, row3))

    intercept[TiBatchWriteException] {
      // insert row2 row4
      tidbWrite(List(row2, row4), schema)
    }
  }

  test("Test no pk adding unique index") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(pk int, i int, s varchar(128), unique index(i))")
    jdbcUpdate(
      s"insert into $dbtable values(1, 1, 'Hello')"
    )

    // insert row2 row3
    tidbWrite(List(row2, row3), schema)
    testTiDBSelect(Seq(row1, row2, row3))

    intercept[TiBatchWriteException] {
      // insert row2 row4
      tidbWrite(List(row2, row4), schema)
    }

    intercept[TiBatchWriteException] {
      // insert row2 row4
      tidbWrite(List(row5, row5), schema)
    }
  }
}
