package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

// without TiExtensions
// will not load tidb_config.properties to SparkConf
class UpsertSuite extends BaseDataSourceSuite("test_datasource_upsert") {
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2, "TiDB")
  private val row3 = Row(3, "Spark")
  private val row4 = Row(4, null)
  private val row5 = Row(5, "Duplicate")

  private val row3_v2 = Row(3, "TiSpark")

  private val schema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("s", StringType)
    )
  )

  test("Test upsert to table without primary key") {
    dropTable()
    jdbcUpdate(s"create table $dbtableInJDBC(i int, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtableInJDBC values(null, 'Hello')"
    )
    // insert row2 row3
    batchWrite(List(row2, row3), schema)
    testSelect(dbtableInSpark, Seq(row1, row2, row3))

    // insert row4
    batchWrite(List(row4), schema)
    testSelect(dbtableInSpark, Seq(row1, row2, row3, row4))

    // deduplicate=false
    // a table does not need to check duplicate if it does not have a primary key
    batchWrite(List(row5, row5), schema, Some(Map("deduplicate" -> "false")))
    testSelect(dbtableInSpark, Seq(row1, row2, row3, row4, row5, row5))

    // test update
    // insert row3_v2
    batchWrite(List(row3_v2), schema)
    testSelect(dbtableInSpark, Seq(row1, row2, row3, row3_v2, row4, row5, row5))
  }

  test("Test upsert to table with primary key (primary key is handle)") {
    dropTable()
    jdbcUpdate(s"create table $dbtableInJDBC(i int primary key, s varchar(128))")
    batchWrite(List(row2, row3, row4), schema)
    testSelect(dbtableInSpark, Seq(row2, row3, row4))

    // deduplicate=false
    // insert row5 row5
    {
      val caught = intercept[TiBatchWriteException] {
        batchWrite(List(row5, row5), schema)
      }
      assert(
        caught.getMessage
          .equals("data conflicts! set the parameter deduplicate.")
      )
    }

    // deduplicate=true
    // insert row5 row5
    batchWrite(List(row5, row5), schema, Some(Map("deduplicate" -> "true")))
    testSelect(dbtableInSpark, Seq(row2, row3, row4, row5))

    // test update
    batchWrite(List(row3_v2), schema)
    testSelect(dbtableInSpark, Seq(row2, row3_v2, row4, row5))
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
