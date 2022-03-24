/*
 * Copyright 2021 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.delete

import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tispark.TiConfigConst.PD_ADDRESSES
import com.pingcap.tispark.datasource.BaseBatchWriteTest
import com.pingcap.tispark.write.{TiDBDelete, TiDBOptions}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.TestConstants.{TiDB_ADDRESS, TiDB_PASSWORD, TiDB_PORT, TiDB_USER}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.Matchers.the
import org.scalatest.Matchers.{convertToAnyShouldWrapper, have}

import java.util.concurrent.{ExecutorService, Executors}

/**
 * Delete conflict test
 * 1. Delete & DDL(change schema)
 * 2. Delete & Read (MVC)
 * 3. Delete & Write
 *
 */
class DeleteConcurrencyTestSuite extends BaseBatchWriteTest("test_delete_concurrency") {

  private val sleepTime = 5000
  val executor: ExecutorService = Executors.newCachedThreadPool()

  // delete && DDL
  test("Delete & DDL conflict") {
    val table = "delete_ddl"
    val dbtable = s"$database.$table"
    jdbcUpdate(s"drop table if exists $dbtable")
    jdbcUpdate(s"create table $dbtable(i int, s int,PRIMARY KEY (i))")
    jdbcUpdate(s"insert into $dbtable values(0,0),(1,1),(2,2),(3,3)")

    val tiDBOptions = getTiDBOptions(sleepTime * 2)
    val df = spark.sql(s"select * from $dbtable")

    // change schema during delete
    executor.execute(() => {
      Thread.sleep(sleepTime)
      jdbcUpdate(s"alter table $dbtable ADD t varchar(255)")
    })

    // throw exception when schema change
    the[TiBatchWriteException] thrownBy {
      TiDBDelete(df, database, table, Some(tiDBOptions)).delete()
    } should have message "schema has changed during prewrite!"

  }

  // delete && read
  test("Delete & Read ") {
    val table = "delete_read"
    val dbtable = s"$database.$table"
    jdbcUpdate(s"drop table if exists $dbtable")
    jdbcUpdate(s"create table $dbtable(i int, s int,PRIMARY KEY (i))")
    jdbcUpdate(s"insert into $dbtable values(0,0),(1,1),(2,2),(3,3)")

    val expected = spark.sql(s"select count(*) from $dbtable").head().get(0)

    val tiDBOptions = getTiDBOptions(sleepTime * 2)
    val df = spark.sql(s"select * from $dbtable")
    executor.execute(() => {
      TiDBDelete(df, database, table, Some(tiDBOptions)).delete()
    })

    // read old value before delete commit
    Thread.sleep(sleepTime)
    val actual = spark.sql(s"select count(*) from $dbtable").head().get(0)
    assert(expected == actual)
  }

  // delete && write without (success)
  test("Delete & Write without conflict") {
    val table = s"delete_write_no_conflict"
    val dbtable = s"$database.$table"
    jdbcUpdate(s"drop table if exists $dbtable")
    jdbcUpdate(s"create table $dbtable(i int, s int,PRIMARY KEY (i))")
    jdbcUpdate(s"insert into $dbtable values(3,3)")

    val schema: StructType =
      StructType(List(StructField("i", IntegerType), StructField("s", IntegerType)))
    val data: RDD[Row] = sc.makeRDD(List(Row(1, 1), Row(2, 2)))
    val writeDf = sqlContext.createDataFrame(data, schema)
    val data2: RDD[Row] = sc.makeRDD(List(Row(3, 3)))
    val deleteDf = sqlContext.createDataFrame(data2, schema)

    executor.execute(() => {
      writeDf.write
        .format("tidb")
        .options(tidbOptions)
        .option("database", database)
        .option("table", table)
        .option("sleepAfterPrewriteSecondaryKey", sleepTime * 10)
        .mode("append")
        .save()
    })

    executor.execute(() => {
      Thread.sleep(sleepTime)
      val tiDBOptions = getTiDBOptions(0)
      TiDBDelete(deleteDf, database, table, Some(tiDBOptions)).delete()
    })

    // delete won't be blocked without conflict
    Thread.sleep(sleepTime * 5)
    val actual = spark.sql(s"select count(*) from $dbtable").head().get(0)
    assert(0 == actual)
  }

  // delete & write with conflict
  // ignore for the bug: delete after write with conflict will retry forever
  ignore("Delete & Write with conflict") {
    val table = s"delete_write_conflict"
    val dbtable = s"$database.$table"
    jdbcUpdate(s"drop table if exists $dbtable")
    jdbcUpdate(s"create table $dbtable(i int, s int,PRIMARY KEY (i))")
    jdbcUpdate(s"insert into $dbtable values(3,3)")

    val schema: StructType =
      StructType(List(StructField("i", IntegerType), StructField("s", IntegerType)))
    val data: RDD[Row] = sc.makeRDD(List(Row(1, 1), Row(2, 2)))
    val writeDf = sqlContext.createDataFrame(data, schema)
    val data2: RDD[Row] = sc.makeRDD(List(Row(1, 1), Row(2, 2), Row(3, 3)))
    val deleteDf = sqlContext.createDataFrame(data2, schema)

    executor.execute(() => {
      writeDf.write
        .format("tidb")
        .options(tidbOptions)
        .option("database", database)
        .option("table", table)
        .option("sleepAfterPrewriteSecondaryKey", sleepTime * 5)
        .mode("append")
        .save()
    })

    executor.execute(() => {
      Thread.sleep(sleepTime)
      val tiDBOptions = getTiDBOptions(0)
      TiDBDelete(deleteDf, database, table, Some(tiDBOptions)).delete()
    })

    // delete during write: will be blocked by write (don't known why)
    Thread.sleep(sleepTime * 2)
    val actual = spark.sql(s"select count(*) from $dbtable").head().get(0)
    assert(1 == actual)

    // delete after write: will fail for the write write conflict
    // but it will retry forever, i think is a bug (so ignore the test before fix it)
    Thread.sleep(sleepTime * 5)
    val actual2 = spark.sql(s"select count(*) from $dbtable").head().get(0)
    assert(3 == actual2)
  }

  private def getTiDBOptions(sleepAfterPrewriteSecondaryKey: Long): TiDBOptions = {
    val options = Map(
      TiDB_ADDRESS -> tidbAddr,
      TiDB_PASSWORD -> tidbPassword,
      TiDB_PORT -> s"$tidbPort",
      TiDB_USER -> tidbUser,
      PD_ADDRESSES -> pdAddresses,
      "database" -> "",
      "table" -> "",
      "sleepAfterPrewriteSecondaryKey" -> sleepAfterPrewriteSecondaryKey.toString)
    new TiDBOptions(options)
  }

}
