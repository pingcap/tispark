package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

// without TiExtensions
// will not load tidb_config.properties to SparkConf
class UpsertSuite extends BaseDataSourceSuite("test_datasource_upsert") {
  // Values used for comparison
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

  override def beforeAll(): Unit = {
    super.beforeAll()

    jdbcUpdate(s"drop table if exists $dbtableInJDBC")
    jdbcUpdate(s"create table $dbtableInJDBC(i int, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtableInJDBC values(null, 'Hello')"
    )
  }

  test("Test upsert to table without primary key") {
    // insert row2 row3
    batchWrite(List(row2, row3), schema)
    testSelect(dbtableInSpark, Seq(row1, row2, row3))

    // insert row4
    batchWrite(List(row4), schema)
    testSelect(dbtableInSpark, Seq(row1, row2, row3, row4))

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
    testSelect(dbtableInSpark, Seq(row1, row2, row3, row4, row5))

    // test update
    // insert row3_v2
    batchWrite(List(row3_v2), schema)
    testSelect(dbtableInSpark, Seq(row1, row2, row3_v2, row4, row5))
  }

  test("Test upsert to table with primary key (primary key is handle)") {
    //TODO
  }

  override def afterAll(): Unit =
    try {
      jdbcUpdate(s"drop table if exists $dbtableInJDBC")
    } finally {
      super.afterAll()
    }
}
