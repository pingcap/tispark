package com.pingcap.tispark.partition

import com.pingcap.tikv.meta.Collation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class PartitionNewCollationSuite extends PartitionBaseSuite {

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  private def checkNewCollationEnabled(): Unit = {
    if (!Collation.isNewCollationEnabled) {
      cancel()
    }
  }

  /**
   * For utf8mb4_bin collation, "A" < "a"
   * For utf8mb4_general_ci collation, "A" = "a"
   */
  test("range column with bin collation \"a\" and \"A\"") {
    checkNewCollationEnabled()
    tidbStmt.execute(
      s"create table `$database`.`$table` (id bigint, name varchar(16) unique key) CHARSET utf8mb4 COLLATE utf8mb4_bin  partition by range columns(name) (" +
        s"partition p0 values less than ('a')," +
        s"partition p1 values less than MAXVALUE)")

    val data: RDD[Row] = sc.makeRDD(List(Row(57L, "A")))
    val schema: StructType =
      StructType(List(StructField("id", LongType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "false")
      .mode("append")
      .save()

    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(
      Row(57L, "A"))
    checkPartitionJDBCResult(
      Map(
        "p0" -> Array(Array(57L, "A")),
        "p1" -> Array()))

    tidbStmt.execute(s"ADMIN CHECK TABLE `$database`.`$table`")
  }

  test("range column with utf8mb4_general_ci collation \"a\" and \"A\"") {
    checkNewCollationEnabled()
    tidbStmt.execute(
      s"create table `$database`.`$table` (id bigint, name varchar(16) unique key) CHARSET utf8mb4 COLLATE utf8mb4_general_ci partition by range columns(name) (" +
        s"partition p0 values less than ('a')," +
        s"partition p1 values less than MAXVALUE)")

    val data: RDD[Row] = sc.makeRDD(List(Row(57L, "A")))
    val schema: StructType =
      StructType(List(StructField("id", LongType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "false")
      .mode("append")
      .save()

    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(
      Row(57L, "A"))
    checkPartitionJDBCResult(
      Map(
        "p0" -> Array(),
        "p1" -> Array(Array(57L, "A"))))

    tidbStmt.execute(s"ADMIN CHECK TABLE `$database`.`$table`")
  }

  /**
   * For utf8mb4_general_ci collation, "ß" < "ss"
   * For utf8mb4_unicode_ci collation, "ß" = "ss"
   */
  test("range column with utf8mb4_general_ci collation \"ss\" and \"ß\"") {
    checkNewCollationEnabled()
    tidbStmt.execute(
      s"create table `$database`.`$table` (id bigint, name varchar(16) unique key) CHARSET utf8mb4 COLLATE utf8mb4_general_ci partition by range columns(name) (" +
        s"partition p0 values less than ('ss')," +
        s"partition p1 values less than MAXVALUE)")

    val data: RDD[Row] = sc.makeRDD(List(Row(57L, "ß")))
    val schema: StructType =
      StructType(List(StructField("id", LongType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "false")
      .mode("append")
      .save()

    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(
      Row(57L, "ß"))
    checkPartitionJDBCResult(
      Map(
        "p0" -> Array(Array(57L, "ß")),
        "p1" -> Array()))

    tidbStmt.execute(s"ADMIN CHECK TABLE `$database`.`$table`")
  }

  test("range column with utf8mb4_unicode_ci collation \"ss\" and \"ß\"") {
    checkNewCollationEnabled()
    tidbStmt.execute(
      s"create table `$database`.`$table` (id bigint, name varchar(16) unique key) CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci partition by range columns(name) (" +
        s"partition p0 values less than ('ss')," +
        s"partition p1 values less than MAXVALUE)")

    val data: RDD[Row] = sc.makeRDD(List(Row(57L, "ß")))
    val schema: StructType =
      StructType(List(StructField("id", LongType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "false")
      .mode("append")
      .save()

    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(
      Row(57L, "ß"))
    checkPartitionJDBCResult(
      Map(
        "p0" -> Array(),
        "p1" -> Array(Array(57L, "ß"))))

    tidbStmt.execute(s"ADMIN CHECK TABLE `$database`.`$table`")
  }
}

