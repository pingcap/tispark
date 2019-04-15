package com.pingcap.tispark.datasource

import com.pingcap.tispark.TiUtils.TIDB_SOURCE_NAME
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}

// without TiExtensions
// will not load tidb_config.properties to SparkConf
class BasicDataSouceSuite extends BaseDataSourceSuite {
  private val testTable: String = "test_data_source_basic"

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

  // calculated var
  private val testDBTableInJDBC = s"$testDatabase.$testTable"
  private var testDBTableInSpark: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    testDBTableInSpark = s"${getTestDatabaseNameInSpark(testDatabase)}.$testTable"

    jdbcUpdate(s"drop table if exists $testDBTableInJDBC")
    jdbcUpdate(s"create table $testDBTableInJDBC(i int, s varchar(128))")
    jdbcUpdate(
      s"insert into $testDBTableInJDBC values(null, 'Hello'), (2, 'TiDB')"
    )
  }

  test("Test Select") {
    testSelect(testDBTableInSpark, Seq(row1, row2))
  }

  test("Test Write Append") {
    val data: RDD[Row] = sc.makeRDD(List(row3, row4))
    val df = sqlContext.createDataFrame(data, schema)

    df.write
      .format(TIDB_SOURCE_NAME)
      .options(tidbOptions)
      .option("dbtable", testDBTableInSpark)
      .mode(SaveMode.Append)
      .save()

    testSelect(testDBTableInSpark, Seq(row1, row2, row3, row4))
  }

  test("Test Write Overwrite") {
    val data: RDD[Row] = sc.makeRDD(List(row3, row4))
    val df = sqlContext.createDataFrame(data, schema)

    df.write
      .format(TIDB_SOURCE_NAME)
      .options(tidbOptions)
      .option("dbtable", testDBTableInSpark)
      .mode(SaveMode.Overwrite)
      .save()

    testSelect(testDBTableInSpark, Seq(row3, row4))
  }

  private def testSelect(dbtable: String, expectedAnswer: Seq[Row]): Unit = {
    val df = sqlContext.read
      .format(TIDB_SOURCE_NAME)
      .options(tidbOptions)
      .option("dbtable", testDBTableInSpark)
      .load()
      .sort("i")

    checkAnswer(df, expectedAnswer)
  }

  override def afterAll(): Unit =
    try {
      jdbcUpdate(s"drop table if exists $testDBTableInJDBC")
    } finally {
      super.afterAll()
    }
}
