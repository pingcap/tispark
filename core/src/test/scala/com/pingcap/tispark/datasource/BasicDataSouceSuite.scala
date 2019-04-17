package com.pingcap.tispark.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}

// without TiExtensions
// will not load tidb_config.properties to SparkConf
class BasicDataSouceSuite extends BaseDataSourceSuite("test_data_source_basic") {
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
      .mode(SaveMode.Append)
      .save()

    testSelect(dbtableInSpark, Seq(row1, row2, row3, row4))
  }

  test("Test Write Overwrite") {
    val data: RDD[Row] = sc.makeRDD(List(row3, row4))
    val df = sqlContext.createDataFrame(data, schema)

    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", databaseInSpark)
      .option("table", testTable)
      .mode(SaveMode.Overwrite)
      .save()

    testSelect(dbtableInSpark, Seq(row3, row4))
  }

  private def testSelect(dbtable: String, expectedAnswer: Seq[Row]): Unit = {
    val df = sqlContext.read
      .format("tidb")
      .options(tidbOptions)
      .option("database", databaseInSpark)
      .option("table", testTable)
      .load()
      .sort("i")

    checkAnswer(df, expectedAnswer)
  }

  override def afterAll(): Unit =
    try {
      jdbcUpdate(s"drop table if exists $dbtableInJDBC")
    } finally {
      super.afterAll()
    }
}
