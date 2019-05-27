package com.pingcap.tispark.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class MissingParameterSuite extends BaseDataSourceTest("test_datasource_missing_parameter") {
  private val row1 = Row(null, "Hello")

  private val schema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("s", StringType)
    )
  )

  test("Missing parameter: database") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")

    val caught = intercept[IllegalArgumentException] {
      val rows = row1 :: Nil
      val data: RDD[Row] = sc.makeRDD(rows)
      val df = sqlContext.createDataFrame(data, schema)
      df.write
        .format("tidb")
        .options(tidbOptions)
        .option("table", table)
        .mode("append")
        .save()
    }
    assert(
      caught.getMessage
        .equals(
          "requirement failed: Option 'database' is required."
        )
    )
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
