package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class DataSourceWithoutExtensionsSuite
    extends BaseDataSourceTest("test_datasource_without_extensions", "tispark_test", false) {
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

  override def beforeAll(): Unit =
    super.beforeAll()

  test("Test Select without extensions") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(null, 'Hello'), (2, 'TiDB'), (3, 'Spark'), (4, null)"
    )
    testTiDBSelect(Seq(row1, row2, row3, row4))
  }

  test("Test Write Append without extensions") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(null, 'Hello'), (2, 'TiDB')"
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

    testTiDBSelect(Seq(row1, row2, row3, row4))
  }

  test("Test Write Overwrite without extensions") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")

    val data: RDD[Row] = sc.makeRDD(List(row3, row4))
    val df = sqlContext.createDataFrame(data, schema)

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

  test("Test Simple Comparisons without extensions") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(null, 'Hello'), (2, 'TiDB'), (3, 'Spark'), (4, null)"
    )
    testTiDBSelectFilter("s = 'Hello'", Seq(row1))
    testTiDBSelectFilter("i > 2", Seq(row3, row4))
    testTiDBSelectFilter("i < 3", Seq(row2))
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
