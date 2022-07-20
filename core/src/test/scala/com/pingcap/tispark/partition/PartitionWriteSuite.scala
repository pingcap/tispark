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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{BaseTiSparkTest, Row}
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, have, the}

import java.sql.{Date, ResultSet, Timestamp}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class PartitionWriteSuite extends BaseTiSparkTest {

  val table: String = "test_partition_write"
  val database: String = "tispark_test"

  override def beforeEach(): Unit = {
    super.beforeEach()
    tidbStmt.execute(s"drop table if exists `$database`.`$table`")
  }

  override def afterEach(): Unit = {
    tidbStmt.execute(s"ADMIN CHECK TABLE `$database`.`$table`")
    super.afterEach()
  }

  /**
   * hash partition test
   * - append and delete
   * - replace and delete
   * - replace and delete with YEAR()
   */
  test("hash partition append and delete test") {
    tidbStmt.execute(
      s"create table `$database`.`$table` (id int) partition by hash(id) PARTITIONS 4")

    val data: RDD[Row] = sc.makeRDD(List(Row(5), Row(35), Row(25), Row(15)))
    val schema: StructType =
      StructType(List(StructField("id", IntegerType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "false")
      .mode("append")
      .save()
    tidbStmt.execute(s"insert into `$database`.`$table` values (6), (7), (28), (29)")

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(
      Row(5),
      Row(6),
      Row(7),
      Row(15),
      Row(25),
      Row(35),
      Row(28),
      Row(29))
    checkJDBCResult(
      insertResultJDBC,
      Array(Array(5), Array(6), Array(7), Array(15), Array(25), Array(35), Array(28), Array(29)))

    spark.sql(s"delete from `tidb_catalog`.`$database`.`$table` where id < 16")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark
      .collect() should contain theSameElementsAs Array(Row(25), Row(35), Row(28), Row(29))
    checkJDBCResult(deleteResultJDBC, Array(Array(25), Array(35), Array(28), Array(29)))
  }

  test("hash partition replace and delete test") {
    tidbStmt.execute(
      s"create table `$database`.`$table` (id bigint primary key , name varchar(16)) partition by hash(id) PARTITIONS 4")

    tidbStmt.execute(
      s"insert into `$database`.`$table` values (5, 'Apple'), (25, 'Honey'), (29, 'Mike')")
    val data: RDD[Row] = sc.makeRDD(List(Row(5L, "Luo"), Row(25L, "John"), Row(15L, "Jack")))
    val schema: StructType =
      StructType(List(StructField("id", LongType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "true")
      .mode("append")
      .save()

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(
      Row(5L, "Luo"),
      Row(25L, "John"),
      Row(15L, "Jack"),
      Row(29, "Mike"))
    checkJDBCResult(
      insertResultJDBC,
      Array(Array(5L, "Luo"), Array(25L, "John"), Array(15L, "Jack"), Array(29, "Mike")))

    spark.sql(s"delete from `tidb_catalog`.`$database`.`$table` where id < 16 or name = 'Mike'")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark.collect() should contain theSameElementsAs Array(Row(25L, "John"))
    checkJDBCResult(deleteResultJDBC, Array(Array(25L, "John")))
  }

  test("hash YEAR() partition replace and delete test") {
    tidbStmt.execute(
      s"create table `$database`.`$table` (birthday date primary key , name varchar(16)) partition by hash(YEAR(birthday)) PARTITIONS 4")

    tidbStmt.execute(
      s"insert into `$database`.`$table` values ('1995-06-15', 'Apple'), ('1995-08-08', 'Honey'), ('1999-06-04', 'Mike')")
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(Date.valueOf("1995-06-15"), "Luo"),
        Row(Date.valueOf("1995-08-08"), "John"),
        Row(Date.valueOf("1993-08-22"), "Jack")))
    val schema: StructType =
      StructType(List(StructField("birthday", DateType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "true")
      .mode("append")
      .save()

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(
      Row(Date.valueOf("1995-06-15"), "Luo"),
      Row(Date.valueOf("1995-08-08"), "John"),
      Row(Date.valueOf("1993-08-22"), "Jack"),
      Row(Date.valueOf("1999-06-04"), "Mike"))
    checkJDBCResult(
      insertResultJDBC,
      Array(
        Array(Date.valueOf("1995-06-15"), "Luo"),
        Array(Date.valueOf("1995-08-08"), "John"),
        Array(Date.valueOf("1993-08-22"), "Jack"),
        Array(Date.valueOf("1999-06-04"), "Mike")))

    spark.sql(
      s"delete from `tidb_catalog`.`$database`.`$table` where birthday <= '1995-06-15' or name = 'Mike'")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark.collect() should contain theSameElementsAs Array(
      Row(Date.valueOf("1995-08-08"), "John"))
    checkJDBCResult(deleteResultJDBC, Array(Array(Date.valueOf("1995-08-08"), "John")))
  }

  /**
   * range column partition
   * - append and delete
   * - replace and delete
   * - date type replace and delete
   * - datetime type replace and delete
   * - binary type replace and delete
   * - varbinary type replace and delete
   */
  test("range column append and delete test") {
    tidbStmt.execute(
      s"create table `$database`.`$table` (id bigint, name varchar(16) unique key) partition by range columns(name) (" +
        s"partition p0 values less than ('BBBBBB')," +
        s"partition p1 values less than ('HHHHHH')," +
        s"partition p2 values less than MAXVALUE)")

    tidbStmt.execute(s"insert into `$database`.`$table` values (29, 'Mike')")
    val data: RDD[Row] = sc.makeRDD(List(Row(57L, "Apple"), Row(65L, "John"), Row(15L, "Jack")))
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

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(
      Row(57L, "Apple"),
      Row(65L, "John"),
      Row(15L, "Jack"),
      Row(29, "Mike"))
    checkJDBCResult(
      insertResultJDBC,
      Array(Array(57L, "Apple"), Array(65L, "John"), Array(15L, "Jack"), Array(29, "Mike")))

    spark.sql(
      s"delete from `tidb_catalog`.`$database`.`$table` where name < 'John' or name = 'Mike'")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark.collect() should contain theSameElementsAs Array(Row(65L, "John"))
    checkJDBCResult(deleteResultJDBC, Array(Array(65L, "John")))
  }

  test("range column replace and delete test") {
    tidbStmt.execute(
      s"create table `$database`.`$table` (id bigint, name varchar(16) unique key) partition by range columns(name) (" +
        s"partition p0 values less than ('BBBBBB')," +
        s"partition p1 values less than ('HHHHHH')," +
        s"partition p2 values less than MAXVALUE)")

    tidbStmt.execute(
      s"insert into `$database`.`$table` values (5, 'Apple'), (25, 'John'), (29, 'Mike')")
    val data: RDD[Row] = sc.makeRDD(List(Row(57L, "Apple"), Row(65L, "John"), Row(15L, "Jack")))
    val schema: StructType =
      StructType(List(StructField("id", LongType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "true")
      .mode("append")
      .save()

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(
      Row(57L, "Apple"),
      Row(65L, "John"),
      Row(15L, "Jack"),
      Row(29, "Mike"))
    checkJDBCResult(
      insertResultJDBC,
      Array(Array(57L, "Apple"), Array(65L, "John"), Array(15L, "Jack"), Array(29, "Mike")))

    spark.sql(
      s"delete from `tidb_catalog`.`$database`.`$table` where name < 'John' or name = 'Mike'")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark.collect() should contain theSameElementsAs Array(Row(65L, "John"))
    checkJDBCResult(deleteResultJDBC, Array(Array(65L, "John")))
  }

  test("dateTime type range column partition replace and delete test") {
    tidbStmt.execute(
      s"create table `$database`.`$table` (birthday datetime primary key , name varchar(16)) partition by range columns(birthday) (" +
        s"partition p0 values less than ('1995-07-17 15:15:15')," +
        s"partition p1 values less than ('1996-01-01 15:15:15')," +
        s"partition p2 values less than MAXVALUE)")

    tidbStmt.execute(
      s"insert into `$database`.`$table` values ('1995-06-15 15:15:15', 'Apple'), ('1995-08-08 15:15:15', 'Honey'), ('1999-06-04 15:15:15', 'Mike')")
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(Timestamp.valueOf("1995-06-15 15:15:15"), "Luo"),
        Row(Timestamp.valueOf("1995-08-08 15:15:15"), "John"),
        Row(Timestamp.valueOf("1993-08-22 15:15:15"), "Jack")))
    val schema: StructType =
      StructType(List(StructField("birthday", TimestampType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "true")
      .mode("append")
      .save()

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(
      Row(Timestamp.valueOf("1995-06-15 15:15:15"), "Luo"),
      Row(Timestamp.valueOf("1995-08-08 15:15:15"), "John"),
      Row(Timestamp.valueOf("1993-08-22 15:15:15"), "Jack"),
      Row(Timestamp.valueOf("1999-06-04 15:15:15"), "Mike"))

    val f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    checkJDBCResult(
      insertResultJDBC,
      Array(
        Array(LocalDateTime.from(f.parse("1995-06-15 15:15:15")), "Luo"),
        Array(LocalDateTime.from(f.parse("1995-08-08 15:15:15")), "John"),
        Array(LocalDateTime.from(f.parse("1993-08-22 15:15:15")), "Jack"),
        Array(LocalDateTime.from(f.parse("1999-06-04 15:15:15")), "Mike")))

    spark.sql(
      s"delete from `tidb_catalog`.`$database`.`$table` where birthday <= '1995-06-15 15:15:15' or name = 'Mike'")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark.collect() should contain theSameElementsAs Array(
      Row(Timestamp.valueOf("1995-08-08 15:15:15"), "John"))
    checkJDBCResult(
      deleteResultJDBC,
      Array(Array(LocalDateTime.from(f.parse("1995-08-08 15:15:15")), "John")))
  }

  test("date type range column partition replace and delete test") {
    tidbStmt.execute(
      s"create table `$database`.`$table` (birthday date primary key , name varchar(16)) partition by range columns(birthday) (" +
        s"partition p0 values less than ('1995-07-17')," +
        s"partition p1 values less than ('1996-01-01')," +
        s"partition p2 values less than MAXVALUE)")

    tidbStmt.execute(
      s"insert into `$database`.`$table` values ('1995-06-15', 'Apple'), ('1995-08-08', 'Honey'), ('1999-06-04', 'Mike')")
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(Date.valueOf("1995-06-15"), "Luo"),
        Row(Date.valueOf("1995-08-08"), "John"),
        Row(Date.valueOf("1993-08-22"), "Jack")))
    val schema: StructType =
      StructType(List(StructField("birthday", DateType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "true")
      .mode("append")
      .save()

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(
      Row(Date.valueOf("1995-06-15"), "Luo"),
      Row(Date.valueOf("1995-08-08"), "John"),
      Row(Date.valueOf("1993-08-22"), "Jack"),
      Row(Date.valueOf("1999-06-04"), "Mike"))
    checkJDBCResult(
      insertResultJDBC,
      Array(
        Array(Date.valueOf("1995-06-15"), "Luo"),
        Array(Date.valueOf("1995-08-08"), "John"),
        Array(Date.valueOf("1993-08-22"), "Jack"),
        Array(Date.valueOf("1999-06-04"), "Mike")))

    spark.sql(
      s"delete from `tidb_catalog`.`$database`.`$table` where birthday <= '1995-06-15' or name = 'Mike'")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark.collect() should contain theSameElementsAs Array(
      Row(Date.valueOf("1995-08-08"), "John"))
    checkJDBCResult(deleteResultJDBC, Array(Array(Date.valueOf("1995-08-08"), "John")))
  }

  test("varbinary type range column replace test") {
    tidbStmt.execute(
      s"create table `$database`.`$table` (id bigint, name varbinary(16) unique key) partition by range columns(name) (" +
        s"partition p0 values less than (X'424242424242')," +
        s"partition p1 values less than (X'525252525252')," +
        s"partition p2 values less than MAXVALUE)")

    tidbStmt.execute(
      s"insert into `$database`.`$table` values (5, 'Apple'), (25, 'John'), (29, 'Mike')")
    val data: RDD[Row] = sc.makeRDD(List(Row(57L, "Apple"), Row(65L, "John"), Row(15L, "Jack")))
    val schema: StructType =
      StructType(List(StructField("id", LongType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "true")
      .mode("append")
      .save()

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect().map { row =>
      Row(row.getLong(0), new String(row.get(1).asInstanceOf[Array[Byte]]))
    } should contain theSameElementsAs Array(
      Row(57L, "Apple"),
      Row(65L, "John"),
      Row(15L, "Jack"),
      Row(29, "Mike"))
    checkJDBCResult(
      insertResultJDBC,
      Array(Array(57L, "Apple"), Array(65L, "John"), Array(15L, "Jack"), Array(29, "Mike")))

    spark.sql(
      s"delete from `tidb_catalog`.`$database`.`$table` where name < 'John' or name = 'Mike'")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark.collect().map { row =>
      Row(row.getLong(0), new String(row.get(1).asInstanceOf[Array[Byte]]))
    } should contain theSameElementsAs Array(Row(65L, "John"))
    checkJDBCResult(deleteResultJDBC, Array(Array(65L, "John")))
  }

  test("binary type range column replace test") {
    tidbStmt.execute(
      s"create table `$database`.`$table` (id bigint, name binary(6) unique key) partition by range columns(name) (" +
        s"partition p0 values less than (X'424242424242')," +
        s"partition p1 values less than (X'525252525252')," +
        s"partition p2 values less than MAXVALUE)")

    tidbStmt.execute(
      s"insert into `$database`.`$table` values (5, 'Apple'), (25, 'John'), (29, 'Mike')")
    val data: RDD[Row] = sc.makeRDD(List(Row(57L, "Apple"), Row(65L, "John"), Row(15L, "Jack")))
    val schema: StructType =
      StructType(List(StructField("id", LongType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "true")
      .mode("append")
      .save()

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect().map { row =>
      Row(row.getLong(0), new String(row.get(1).asInstanceOf[Array[Byte]]))
    } should contain theSameElementsAs Array(
      Row(57L, "Apple\u0000"),
      Row(65L, "John\u0000\u0000"),
      Row(15L, "Jack\u0000\u0000"),
      Row(29, "Mike\u0000\u0000"))
    checkJDBCResult(
      insertResultJDBC,
      Array(
        Array(57L, "Apple\u0000"),
        Array(65L, "John\u0000\u0000"),
        Array(15L, "Jack\u0000\u0000"),
        Array(29, "Mike\u0000\u0000")))

    spark.sql(
      s"delete from `tidb_catalog`.`$database`.`$table` where name < 'John\u0000\u0000' or name = 'Mike\u0000\u0000'")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark.collect().map { row =>
      Row(row.getLong(0), new String(row.get(1).asInstanceOf[Array[Byte]]))
    } should contain theSameElementsAs Array(Row(65L, "John\u0000\u0000"))
    checkJDBCResult(deleteResultJDBC, Array(Array(65L, "John\u0000\u0000")))
  }

  /**
   * range partition
   * - append and delete
   * - replace and delete
   * - replace and delete with YEAR()
   */
  test("range append and delete test") {
    tidbStmt.execute(
      s"create table `$database`.`$table` (id bigint primary key, name varchar(16)) partition by range (id) (" +
        s"partition p0 values less than (20)," +
        s"partition p1 values less than (60)," +
        s"partition p2 values less than MAXVALUE)")

    tidbStmt.execute(s"insert into `$database`.`$table` values (29, 'Mike')")
    val data: RDD[Row] = sc.makeRDD(List(Row(57L, "Apple"), Row(65L, "John"), Row(15L, "Jack")))
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

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(
      Row(57L, "Apple"),
      Row(65L, "John"),
      Row(15L, "Jack"),
      Row(29, "Mike"))
    checkJDBCResult(
      insertResultJDBC,
      Array(Array(57L, "Apple"), Array(65L, "John"), Array(15L, "Jack"), Array(29, "Mike")))

    spark.sql(
      s"delete from `tidb_catalog`.`$database`.`$table` where name < 'John' or name = 'Mike'")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark.collect() should contain theSameElementsAs Array(Row(65L, "John"))
    checkJDBCResult(deleteResultJDBC, Array(Array(65L, "John")))
  }

  test("range replace and delete test") {
    tidbStmt.execute(
      s"create table `$database`.`$table` (id bigint primary key, name varchar(16)) partition by range (id) (" +
        s"partition p0 values less than (20)," +
        s"partition p1 values less than (60)," +
        s"partition p2 values less than MAXVALUE)")

    tidbStmt.execute(
      s"insert into `$database`.`$table` values (57, 'Hello'), (65, 'Amount'), (29, 'Mike')")
    val data: RDD[Row] = sc.makeRDD(List(Row(57L, "Apple"), Row(65L, "John"), Row(15L, "Jack")))
    val schema: StructType =
      StructType(List(StructField("id", LongType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "true")
      .mode("append")
      .save()

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(
      Row(57L, "Apple"),
      Row(65L, "John"),
      Row(15L, "Jack"),
      Row(29, "Mike"))
    checkJDBCResult(
      insertResultJDBC,
      Array(Array(57L, "Apple"), Array(65L, "John"), Array(15L, "Jack"), Array(29, "Mike")))

    spark.sql(s"delete from `tidb_catalog`.`$database`.`$table` where id < 50 or name = 'Apple'")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark.collect() should contain theSameElementsAs Array(Row(65L, "John"))
    checkJDBCResult(deleteResultJDBC, Array(Array(65L, "John")))
  }

  test("range YEAR() partition replace and delete test") {
    tidbStmt.execute(
      s"create table `$database`.`$table` (birthday date primary key , name varchar(16)) partition by range(YEAR(birthday)) (" +
        s"partition p0 values less than (1995)," +
        s"partition p1 values less than (YEAR('1997-01-01'))," +
        s"partition p2 values less than MAXVALUE)")

    tidbStmt.execute(
      s"insert into `$database`.`$table` values ('1995-06-15', 'Apple'), ('1995-08-08', 'Honey'), ('1999-06-04', 'Mike')")
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(Date.valueOf("1995-06-15"), "Luo"),
        Row(Date.valueOf("1995-08-08"), "John"),
        Row(Date.valueOf("1993-08-22"), "Jack")))
    val schema: StructType =
      StructType(List(StructField("birthday", DateType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "true")
      .mode("append")
      .save()

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(
      Row(Date.valueOf("1995-06-15"), "Luo"),
      Row(Date.valueOf("1995-08-08"), "John"),
      Row(Date.valueOf("1993-08-22"), "Jack"),
      Row(Date.valueOf("1999-06-04"), "Mike"))
    checkJDBCResult(
      insertResultJDBC,
      Array(
        Array(Date.valueOf("1995-06-15"), "Luo"),
        Array(Date.valueOf("1995-08-08"), "John"),
        Array(Date.valueOf("1993-08-22"), "Jack"),
        Array(Date.valueOf("1999-06-04"), "Mike")))

    spark.sql(
      s"delete from `tidb_catalog`.`$database`.`$table` where birthday <= '1995-06-15' or name = 'Mike'")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark.collect() should contain theSameElementsAs Array(
      Row(Date.valueOf("1995-08-08"), "John"))
    checkJDBCResult(deleteResultJDBC, Array(Array(Date.valueOf("1995-08-08"), "John")))
  }

  test("unsupported function UNIX_TIMESTAMP() and range partition replace and delete test") {
    tidbStmt.execute(
      s"create table `$database`.`$table` (birthday timestamp primary key , name varchar(16)) partition by range(UNIX_TIMESTAMP(birthday)) (" +
        s"partition p0 values less than (UNIX_TIMESTAMP('1995-07-07 20:20:20'))," +
        s"partition p1 values less than (UNIX_TIMESTAMP('1996-07-07 20:20:20'))," +
        s"partition p2 values less than MAXVALUE)")

    tidbStmt.execute(
      s"insert into `$database`.`$table` values ('1995-06-15 20:20:20', 'Apple'), ('1995-08-08 20:20:20', 'Honey'), ('1999-06-04 20:20:20', 'Mike')")
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(Timestamp.valueOf("1995-06-15 20:20:20"), "Luo"),
        Row(Timestamp.valueOf("1995-08-08 20:20:20"), "John"),
        Row(Timestamp.valueOf("1993-08-22 20:20:20"), "Jack")))
    val schema: StructType =
      StructType(List(StructField("birthday", TimestampType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)

    the[UnsupportedOperationException] thrownBy {
      df.write
        .format("tidb")
        .options(tidbOptions)
        .option("database", database)
        .option("table", table)
        .option("replace", "true")
        .mode("append")
        .save()
    } should have message s"Unsupported function: UNIX_TIMESTAMP"
  }

  def checkJDBCResult(resultJDBC: ResultSet, rows: Array[Array[_]]): Unit = {
    val rsMetaData = resultJDBC.getMetaData
    var sqlData: Seq[Seq[AnyRef]] = Seq()
    while (resultJDBC.next()) {
      var row: Seq[AnyRef] = Seq()
      for (i <- 1 to rsMetaData.getColumnCount) {
        resultJDBC.getObject(i) match {
          case x: Array[Byte] => row = row :+ new String(x)
          case _ => row = row :+ resultJDBC.getObject(i)
        }
      }
      sqlData = sqlData :+ row
    }
    sqlData should contain theSameElementsAs rows
  }
}
