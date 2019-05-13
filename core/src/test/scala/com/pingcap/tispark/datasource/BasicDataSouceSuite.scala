package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

// without TiExtensions
// will not load tidb_config.properties to SparkConf
class BasicDataSourceSuite extends BaseDataSourceSuite("test_datasource_basic") {
  // Values used for comparison
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2, "TiDB")
  private val row3 = Row(3, "Spark")
  private val row4 = Row(4, null)

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
      s"insert into $dbtableInJDBC values(null, 'Hello'), (2, 'TiDB')"
    )
  }

  test("Test Select") {
    testSelect(dbtableInSpark, Seq(row1, row2))
  }

  test("Test Write Append") {
    val data: RDD[Row] = sc.makeRDD(List(row3, row4))
    val df = sqlContext.createDataFrame(data, schema)

    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", databaseInSpark)
      .option("table", testTable)
      .mode("append")
      .save()

    testSelect(dbtableInSpark, Seq(row1, row2, row3, row4))
  }

  test("Test Write Overwrite") {
    val data: RDD[Row] = sc.makeRDD(List(row3, row4))
    val df = sqlContext.createDataFrame(data, schema)

    val caught = intercept[TiBatchWriteException] {
      df.write
        .format("tidb")
        .options(tidbOptions)
        .option("database", databaseInSpark)
        .option("table", testTable)
        .mode("overwrite")
        .save()
    }

    assert(
      caught.getMessage
        .equals("SaveMode: Overwrite is not supported. TiSpark only support SaveMode.Append.")
    )
  }

  override def afterAll(): Unit =
    try {
      jdbcUpdate(s"drop table if exists $dbtableInJDBC")
    } finally {
      super.afterAll()
    }
}
