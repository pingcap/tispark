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

package com.pingcap.tispark.concurrency

import java.util.concurrent.atomic.AtomicInteger

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class ConcurrencyTest extends BaseDataSourceTest("test_concurrency_write_read") {

  protected val row1: Row = Row(1, "Hello")
  protected val row2: Row = Row(2, "TiDB")
  protected val row3: Row = Row(3, "Spark")
  protected val row4: Row = Row(4, "null")
  protected val row5: Row = Row(5, "test")

  protected val schema: StructType = StructType(
    List(StructField("i", IntegerType), StructField("s", StringType)))

  protected val sleepBeforeQuery = 10000
  protected val sleepAfterPrewriteSecondaryKey = 240000

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
  }

  override protected def dropTable(): Unit = {
    try {
      jdbcUpdate(s"admin cleanup table lock $dbtable")
    } catch {
      case _: Throwable =>
    }

    jdbcUpdate(s"drop table if exists $dbtable")
  }

  protected def newJDBCReadThread(i: Int, resultRowCount: AtomicInteger): Thread = {
    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(sleepBeforeQuery)
        logger.info(s"readThread$i: start query via jdbc")
        try {
          val result = queryTiDBViaJDBC(
            s"select * from $dbtable where i = $i",
            retryOnFailure = 1,
            tidbConn.createStatement())
          logger.info(s"readThread$i:" + result)
          resultRowCount.addAndGet(result.size)
        } catch {
          case e: Throwable => logger.info(s"readThread$i: jdbc with error", e)
        }
      }
    })
  }

  protected def newTiSparkReadThread(i: Int, resultRowCount: AtomicInteger): Thread = {
    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(sleepBeforeQuery)
        logger.info(s"readThread$i: start query via tispark")
        try {
          val result = queryViaTiSpark(s"select * from $dbtableWithPrefix where i = $i")
          logger.info(s"readThread$i:" + result)
          resultRowCount.addAndGet(result.size)
        } catch {
          case e: Throwable => logger.info(s"readThread$i: tispark with error", e)
        }
      }
    })
  }

  protected def doBatchWriteInBackground(options: Map[String, String] = Map.empty): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        logger.info("start doBatchWriteInBackground")
        val data: RDD[Row] = sc.makeRDD(List(row1, row2, row3))
        val df = sqlContext.createDataFrame(data, schema)
        df.write
          .format("tidb")
          .options(tidbOptions)
          .options(options)
          .option("database", database)
          .option("table", table)
          .option("sleepAfterPrewriteSecondaryKey", sleepAfterPrewriteSecondaryKey)
          .mode("append")
          .save()
      }
    }).start()
  }

  protected def compareSelect(): Unit = {
    val query = s"select * from $dbtableWithPrefix order by i"

    sql(query).show(false)

    val r1 = queryViaTiSpark(s"select * from $dbtableWithPrefix order by i")
    val r2 = queryTiDBViaJDBC(s"select * from $dbtable order by i")
    compSqlResult(query, r1, r2, checkLimit = false)
  }
}
