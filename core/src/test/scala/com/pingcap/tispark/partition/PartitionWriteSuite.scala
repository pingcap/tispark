package com.pingcap.tispark.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{BaseTiSparkTest, Row}

class PartitionWriteSuite extends BaseTiSparkTest {

  val table: String = "test_partition_write"
  val database: String = "tispark_test"

  override def beforeEach(): Unit = {
    super.beforeEach()
    tidbStmt.execute(s"drop table if exists `$database`.`$table`")
  }

  test("hash partiton") {
    tidbStmt.execute(s"create table `$database`.`$table` (id int) partition by hash(id) PARTITIONS 4")
    val data: RDD[Row] = sc.makeRDD(List(Row(5), Row(35), Row(25), Row(15)))
    val schema: StructType =
      StructType(List(StructField("id", IntegerType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("sleepAfterGetCommitTS", 20000L)
      .option("replace", "true")
      .mode("append")
      .save()
    val resultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val resultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    print("ss")
  }
}
