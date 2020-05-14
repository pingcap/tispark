package com.pingcap.tispark.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class UpperCaseColumnNameSuite
    extends BaseDataSourceTest("test_datasource_uppser_case_column_name") {

  private val row1 = Row(1, 2)

  private val schema = StructType(
    List(
      StructField("O_ORDERKEY", IntegerType),
      StructField("O_CUSTKEY", IntegerType)
    )
  )

  override def beforeAll(): Unit = {
    super.beforeAll()

    dropTable()
    jdbcUpdate(s"""
                  |CREATE TABLE $dbtable (O_ORDERKEY INTEGER NOT NULL,
                  |                       O_CUSTKEY INTEGER NOT NULL);
       """.stripMargin)
  }

  test("Test insert upper case column name") {
    if (!supportBatchWrite) {
      cancel
    }

    val data: RDD[Row] = sc.makeRDD(List(row1))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .mode("append")
      .save()
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
