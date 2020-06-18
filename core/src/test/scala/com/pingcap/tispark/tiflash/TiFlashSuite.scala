/*
 * Copyright 2020 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.tiflash

import com.pingcap.tispark.TiConfigConst
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{BaseTiSparkTest, Row}

class TiFlashSuite extends BaseTiSparkTest {

  private val row1 = Row(1, "Value1")
  private val row2 = Row(2, "Value2")

  private val schema: StructType = StructType(
    List(StructField("i", IntegerType), StructField("s", StringType)))

  private val sleepBeforeQuery = 10000
  private val sleepAfterPrewriteSecondaryKey = 240000

  test("lock on tiflash: not expired") {
    if (!enableTiFlashTest) {
      cancel("tiflash test not enabled")
    }

    if (!supportBatchWrite) {
      cancel
    }

    dropTable()

    tidbStmt.execute("create table t(i int, s varchar(128))")

    tidbStmt.execute("ALTER TABLE t SET TIFLASH REPLICA 1")

    tidbStmt.execute("insert into t values(1, 'v1')")

    // ttl will not be updated, which means ttl will not expired
    doBatchWriteInBackground()

    Thread.sleep(sleepBeforeQuery)

    queryViaTiflash(() => {
      spark.sql("select * from t").show(false)
    })

    judge("select * from t order by i", canTestTiFlash = true)
  }

  test("lock on tiflash: expired") {
    if (!enableTiFlashTest) {
      cancel("tiflash test not enabled")
    }

    if (!supportBatchWrite) {
      cancel
    }

    dropTable()

    tidbStmt.execute("create table t(i int, s varchar(128))")

    tidbStmt.execute("ALTER TABLE t SET TIFLASH REPLICA 1")

    tidbStmt.execute("insert into t values(1, 'v1')")

    // ttl will not be updated & ttl=1 second, which means ttl will be expired after 1 second
    doBatchWriteInBackground(Map("ttlMode" -> "FIXED", "lockTTLSeconds" -> "1"))

    Thread.sleep(sleepBeforeQuery)

    queryViaTiflash(() => {
      spark.sql("select * from t").show(false)
    })

    judge("select * from t order by i", canTestTiFlash = true)
  }

  private def queryViaTiflash(func: () => scala.Any): Unit = {
    val prev = spark.conf.getOption(TiConfigConst.ISOLATION_READ_ENGINES)
    spark.conf
      .set(TiConfigConst.ISOLATION_READ_ENGINES, TiConfigConst.TIFLASH_STORAGE_ENGINE)
    try {
      func.apply()
    } finally {
      spark.conf.set(
        TiConfigConst.ISOLATION_READ_ENGINES,
        prev.getOrElse(TiConfigConst.DEFAULT_STORAGE_ENGINES))
    }
  }

  private def doBatchWriteInBackground(options: Map[String, String] = Map.empty): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        logger.info("start doBatchWriteInBackground")
        val data: RDD[Row] = sc.makeRDD(List(row1, row2))
        val df = sqlContext.createDataFrame(data, schema)
        df.write
          .format("tidb")
          .options(tidbOptions)
          .options(options)
          .option("database", "tispark_test")
          .option("table", "t")
          .option("sleepAfterPrewriteSecondaryKey", sleepAfterPrewriteSecondaryKey)
          .option("replace", "true")
          .mode("append")
          .save()
      }
    }).start()
  }

  private def dropTable(): Unit = {
    try {
      tidbStmt.execute(s"admin cleanup table lock t")
    } catch {
      case _: Throwable =>
    }

    tidbStmt.execute(s"drop table if exists t")
  }

  override def afterAll(): Unit = {
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
  }
}
