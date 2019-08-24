package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class BasicDataSourceSuite extends BaseDataSourceTest {
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

  test("Test Select") {
    val table = "test_select"
    dropTable(table)
    createTable("create table `%s`.`%s`(i int, s varchar(128))", table)
    jdbcUpdate(
      "insert into `%s`.`%s` values(null, 'Hello'), (2, 'TiDB')",
      table
    )
    testTiDBSelectWithTable(Seq(row1, row2), tableName = table)
  }

  test("Test Write Append") {
    val table = "test_append"
    dropTable(table)
    createTable("create table `%s`.`%s`(i int, s varchar(128))", table)
    jdbcUpdate(
      "insert into `%s`.`%s` values(null, 'Hello'), (2, 'TiDB')",
      table
    )
    val data: RDD[Row] = sc.makeRDD(List(row3, row4))
    val df = sqlContext.createDataFrame(data, schema)

    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .mode("append")
      .save()

    testTiDBSelectWithTable(Seq(row1, row2, row3, row4), tableName = table)
  }

  test("Test Write Overwrite") {
    val data: RDD[Row] = sc.makeRDD(List(row3, row4))
    val df = sqlContext.createDataFrame(data, schema)
    val table = "test_write_overwrite"
    dropTable(table)
    createTable("create table `%s`.`%s`(i int, s varchar(128))", table)
    jdbcUpdate(
      "insert into `%s`.`%s` values(null, 'Hello'), (2, 'TiDB')",
      table
    )
    val caught = intercept[TiBatchWriteException] {
      df.write
        .format("tidb")
        .options(tidbOptions)
        .option("database", database)
        .option("table", table)
        .mode("overwrite")
        .save()
    }

    assert(
      caught.getMessage
        .equals("SaveMode: Overwrite is not supported. TiSpark only support SaveMode.Append.")
    )
  }

}
