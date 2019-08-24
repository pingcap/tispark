package com.pingcap.tispark.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class OnlyOnePkSuite extends BaseDataSourceTest {
  private val row3 = Row(3)
  private val row4 = Row(4)

  private val schema = StructType(
    List(
      StructField("i", IntegerType)
    )
  )

  test("Test Write Append") {
    val data: RDD[Row] = sc.makeRDD(List(row3, row4))
    val df = sqlContext.createDataFrame(data, schema)
    val table = "test_datasource_only_one_pk"

    dropTable(table)
    createTable("create table `%s`.`%s`(i int primary key)", table)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .mode("append")
      .save()

    testTiDBSelectWithTable(Seq(row3, row4), tableName = table)
  }
}
