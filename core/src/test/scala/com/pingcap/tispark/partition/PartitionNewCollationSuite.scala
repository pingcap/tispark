/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.partition

import com.pingcap.tikv.meta.Collation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.Matchers
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
    insertResultSpark.collect() should contain theSameElementsAs Array(Row(57L, "A"))
    checkPartitionJDBCResult(Map("p0" -> Array(Array(57L, "A")), "p1" -> Array()))

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
    insertResultSpark.collect() should contain theSameElementsAs Array(Row(57L, "A"))
    checkPartitionJDBCResult(Map("p0" -> Array(), "p1" -> Array(Array(57L, "A"))))

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
    insertResultSpark.collect() should contain theSameElementsAs Array(Row(57L, "ß"))
    checkPartitionJDBCResult(Map("p0" -> Array(Array(57L, "ß")), "p1" -> Array()))

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
    insertResultSpark.collect() should contain theSameElementsAs Array(Row(57L, "ß"))
    checkPartitionJDBCResult(Map("p0" -> Array(), "p1" -> Array(Array(57L, "ß"))))

    tidbStmt.execute(s"ADMIN CHECK TABLE `$database`.`$table`")
  }

  test("range column read with utf8mb4_unicode_ci collation") {
    checkNewCollationEnabled()
    tidbStmt.execute(
      s"create table `$database`.`$table` (id bigint, name varchar(16) unique key) CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci partition by range columns(name) (" +
        s"partition p0 values less than ('d')," +
        s"partition p1 values less than ('t')," +
        s"partition p2 values less than MAXVALUE)")

    tidbStmt.execute(s"insert into `$database`.`$table` VALUES (17, 'f')")

    // tispark will prune partition ang query p0, p1
    val insertResultSpark =
      spark.sql(s"select * from `tidb_catalog`.`$database`.`$table` where name < 'G'")
    insertResultSpark.collect() should contain theSameElementsAs Array(Row(17L, "f"))
    checkPartitionJDBCResult(Map("p0" -> Array(), "p1" -> Array(Array(17L, "f"))))

    tidbStmt.execute(s"ADMIN CHECK TABLE `$database`.`$table`")
  }

  test("range column read with utf8mb4_bin collation") {
    checkNewCollationEnabled()
    tidbStmt.execute(
      s"create table `$database`.`$table` (id bigint, name varchar(16) unique key) CHARSET utf8mb4 COLLATE utf8mb4_bin partition by range columns(name) (" +
        s"partition p0 values less than ('d')," +
        s"partition p1 values less than ('t')," +
        s"partition p2 values less than MAXVALUE)")

    tidbStmt.execute(s"insert into `$database`.`$table` VALUES (17, 'f')")

    // tispark will prune partition ang only query p0
    val insertResultSpark =
      spark.sql(s"select * from `tidb_catalog`.`$database`.`$table` where name < 'G'")
    insertResultSpark.collect() shouldBe Matchers.empty
    checkPartitionJDBCResult(Map("p0" -> Array(), "p1" -> Array(Array(17L, "f"))))

    tidbStmt.execute(s"ADMIN CHECK TABLE `$database`.`$table`")
  }
}
