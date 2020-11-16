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

package com.pingcap.tispark.ttl

import com.pingcap.tispark.datasource.BaseBatchWriteWithoutDropTableTest
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class InitialTTLExpiredSuite
    extends BaseBatchWriteWithoutDropTableTest("test_initial_ttl_expired") {

  override def beforeAll(): Unit = {
    super.beforeAll()
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
  }

  test("Test Initial TTL Expired") {
    if (!supportTTLUpdate || !supportBatchWrite) {
      cancel
    }

    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          try {
            tidbStmt.execute(s"select * from $dbtable")
            Thread.sleep(1000)
          } catch {
            case _: Throwable =>
          }
        }
      }
    }).start()

    val row1 = Row(1, "Value1")
    val row2 = Row(2, "Value2")

    val schema: StructType =
      StructType(List(StructField("i", IntegerType), StructField("s", StringType)))

    val sleepBeforePrewritePrimaryKey = 30000
    val sleepAfterPrewritePrimaryKey = 5000
    val data: RDD[Row] = sc.makeRDD(List(row1, row2))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("sleepBeforePrewritePrimaryKey", sleepBeforePrewritePrimaryKey)
      .option("sleepAfterPrewritePrimaryKey", sleepAfterPrewritePrimaryKey)
      .mode("append")
      .save()
  }
}
