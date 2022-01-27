package com.pingcap.tispark.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BinaryType, ByteType, IntegerType, StringType, StructField, StructType}

class DeleteSuite extends BaseBatchWriteTest("test_datasource_delete"){

  test("delete cluster index table with int pk") {
    jdbcUpdate(s"drop table if exists $dbtableWithPrefix")
    jdbcUpdate(s"create table $dbtableWithPrefix(i int, s int,PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(s"insert into $dbtableWithPrefix values(0, 0),(1,1)")

    val row1 = Row(0,1)
    val row2 = Row(0,2)
    val data: RDD[Row] = sc.makeRDD(List(row1,row2))
    val schema = StructType(
      List(StructField("i", IntegerType), StructField("s", IntegerType)))
    val df = sqlContext.createDataFrame(data, schema)

    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("delete","true")
      .mode("append")
      .save()

    assert(1 == spark.sql(s"select count(*) from $dbtableWithPrefix").head().get(0))
  }

  test("delete cluster index table with varchar pk") {
    jdbcUpdate(s"drop table if exists $dbtableWithPrefix")
    jdbcUpdate(s"create table $dbtableWithPrefix(i varchar(64), s int,PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(s"insert into $dbtableWithPrefix values('0', 0),('1',1)")

    val row1 = Row("0",1)
    val row2 = Row("0",2)
    val data: RDD[Row] = sc.makeRDD(List(row1,row2))
    val schema = StructType(
      List(StructField("i", StringType), StructField("s", IntegerType)))
    val df = sqlContext.createDataFrame(data, schema)

    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("delete","true")
      .mode("append")
      .save()

    assert(1 == spark.sql(s"select count(*) from $dbtableWithPrefix").head().get(0))
  }

  test("delete noncluster index table with int pk") {
    jdbcUpdate(s"drop table if exists $dbtableWithPrefix")
    jdbcUpdate(s"create table $dbtableWithPrefix(i int, s int,PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(s"insert into $dbtableWithPrefix values(0, 0),(1,1)")

    val row1 = Row(0,1)
    val row2 = Row(0,2)
    val data: RDD[Row] = sc.makeRDD(List(row1,row2))
    val schema = StructType(
      List(StructField("i", IntegerType), StructField("s", IntegerType)))
    val df = sqlContext.createDataFrame(data, schema)


    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("delete","true")
      .mode("append")
      .save()

    val num = spark.sql(s"select count(*) from $dbtableWithPrefix").head().get(0)
    assert(num == 1)
  }

  test("delete noncluster index table with varchar pk") {
    jdbcUpdate(s"drop table if exists $dbtableWithPrefix")
    jdbcUpdate(s"create table $dbtableWithPrefix(i varchar(64), s int,PRIMARY KEY (i)/*T![clustered_index] NONCLUSTERED */)")
    jdbcUpdate(s"insert into $dbtableWithPrefix values('0', 0),('1',1)")

    val row1 = Row("0",1)
    val row2 = Row("0",2)
    val data: RDD[Row] = sc.makeRDD(List(row1,row2))
    val schema = StructType(
      List(StructField("i", StringType), StructField("s", IntegerType)))
    val df = sqlContext.createDataFrame(data, schema)

    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("delete","true")
      .mode("append")
      .save()

    val num = spark.sql(s"select count(*) from $dbtableWithPrefix").head().get(0)
    assert(num == 1)
  }

}
