package com.pingcap.tispark.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class UpperCaseColumnNameSuite extends BaseDataSourceTest {

  private val row1 = Row(1, 2)

  private val schema = StructType(
    List(
      StructField("O_ORDERKEY", IntegerType),
      StructField("O_CUSTKEY", IntegerType)
    )
  )

  test("Test insert upper case column name") {
    val table = "upper_case_col_name"
    dropTable(table)
    createTable(
      """
        |CREATE TABLE `%s`.`%s` (O_ORDERKEY INTEGER NOT NULL,
        |                       O_CUSTKEY INTEGER NOT NULL);
       """.stripMargin,
      table
    )
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
}
