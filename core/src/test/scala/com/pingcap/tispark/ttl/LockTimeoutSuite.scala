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

import com.pingcap.tikv.TTLManager
import com.pingcap.tikv.exception.GrpcException
import com.pingcap.tispark.datasource.BaseBatchWriteWithoutDropTableTest
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class LockTimeoutSuite extends BaseBatchWriteWithoutDropTableTest("test_lock_timeout") {
  private val row1 = Row(1, "Hello")

  private val schema = StructType(
    List(StructField("i", IntegerType), StructField("s", StringType)))

  override def beforeAll(): Unit = {
    super.beforeAll()
    jdbcUpdate(s"drop table if exists $dbtable")
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
  }

  test("Test Lock TTL Timeout") {
    if (!supportTTLUpdate) {
      cancel
    }

    val seconds = 1000
    val sleep1 = TTLManager.MANAGED_LOCK_TTL + 10 * seconds
    val sleep2 = TTLManager.MANAGED_LOCK_TTL + 15 * seconds

    val data: RDD[Row] = sc.makeRDD(List(row1))
    val df = sqlContext.createDataFrame(data, schema)

    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(sleep1)
        queryTiDBViaJDBC(s"select * from $dbtable")
      }
    }).start()

    val grpcException = intercept[GrpcException] {
      df.write
        .format("tidb")
        .options(tidbOptions)
        .option("database", database)
        .option("table", table)
        .option("sleepAfterPrewritePrimaryKey", sleep2)
        .option("commitPrimaryKeyRetryNumber", 1)
        .mode("append")
        .save()
    }

    assert(grpcException.getMessage.equals("retry is exhausted."))
    assert(grpcException.getCause.getMessage.startsWith("Txn commit primary key failed"))
    assert(
      grpcException.getCause.getCause.getMessage.startsWith(
        "Key exception occurred and the reason is retryable: \"Txn(Mvcc(TxnLockNotFound"))
  }
}
