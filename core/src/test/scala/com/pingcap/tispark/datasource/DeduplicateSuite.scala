package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class DeduplicateSuite extends BaseDataSourceSuite("test_datasource_deduplicate") {
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2, "TiDB")
  private val row3 = Row(3, "Spark")
  private val row4 = Row(4, null)
  private val row5 = Row(5, "Duplicate")

  private val schema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("s", StringType)
    )
  )

  test("Test deduplicate (table without primary key)") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(null, 'Hello')"
    )

    // insert row2 row2 row3
    batchWrite(List(row2, row2, row3), schema, Some(Map("deduplicate" -> "true")))
    testSelect(Seq(row1, row2, row2, row3))

    // insert row4 row4 row5
    batchWrite(List(row4, row4, row5), schema, Some(Map("deduplicate" -> "false")))
    testSelect(Seq(row1, row2, row2, row3, row4, row4, row5))
  }

  test("Test deduplicate (table with primary key & primary key is handle)") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i int primary key, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(2, 'TiDB')"
    )

    // deduplicate=false
    // insert row4 row5 row3 row5
    {
      val caught = intercept[TiBatchWriteException] {
        batchWrite(List(row4, row5, row3, row5), schema, Some(Map("deduplicate" -> "false")))
      }
      assert(
        caught.getMessage
          .equals("data conflicts! set the parameter deduplicate.")
      )
    }
    testSelect(Seq(row2))

    // deduplicate=true
    // insert row4 row5 row3 row5
    batchWrite(List(row4, row5, row3, row5), schema, Some(Map("deduplicate" -> "true")))
    testSelect(Seq(row2, row3, row4, row5))
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
