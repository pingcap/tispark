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

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class ConcurrencyTestResult(
    var hasError: Boolean = false,
    var error: Throwable = null,
    var isEmpty: Boolean = true,
    var obj: String = null)

class ConcurrencyTest extends BaseDataSourceTest("test_concurrency_write_read") {

  protected val row1 = Row(1, "Value1")
  protected val row2 = Row(2, "Value2")
  protected val row3 = Row(3, "Value3")
  protected val row4 = Row(4, "Value4")
  protected val row5 = Row(5, "Value5")

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

  protected def newJDBCReadThread(i: Int, res: ConcurrencyTestResult): Thread = {
    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(sleepBeforeQuery)
        logger.info(s"readThread$i: start query via jdbc")
        try {
          val result = queryTiDBViaJDBC(
            s"select s from $dbtable where i = $i",
            retryOnFailure = 1,
            tidbConn.createStatement())
          logger.info(s"readThread$i:" + result)
          res.hasError = false
          if (result.isEmpty) {
            res.isEmpty = true
          } else {
            res.isEmpty = false
            res.obj = result.head.head.toString
          }
        } catch {
          case e: Throwable =>
            res.hasError = true
            res.error = e
            logger.info(s"readThread$i: jdbc with error", e)
        }
      }
    })
  }

  protected def newTiSparkReadThread(i: Int, res: ConcurrencyTestResult): Thread = {
    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(sleepBeforeQuery)
        logger.info(s"readThread$i: start query via tispark")
        try {
          val result = queryViaTiSpark(s"select s from $dbtableWithPrefix where i = $i")
          logger.info(s"readThread$i:" + result)
          res.hasError = false
          if (result.isEmpty) {
            res.isEmpty = true
          } else {
            res.isEmpty = false
            res.obj = result.head.head.toString
          }
        } catch {
          case e: Throwable =>
            res.hasError = true
            res.error = e
            logger.info(s"readThread$i: tispark with error", e)
        }
      }
    })
  }

  protected def doBatchWriteInBackground(options: Map[String, String] = Map.empty): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        logger.info("start doBatchWriteInBackground")
        val data: RDD[Row] = sc.makeRDD(List(row1, row2))
        val df = sqlContext.createDataFrame(data, schema)
        df.write
          .format("tidb")
          .options(tidbOptions)
          .options(options)
          .option("database", database)
          .option("table", table)
          .option("sleepAfterPrewriteSecondaryKey", sleepAfterPrewriteSecondaryKey)
          .option("replace", "true")
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
