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

package com.pingcap.tispark.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class OnlyOnePkSuite extends BaseBatchWriteWithoutDropTableTest("test_datasource_only_one_pk") {
  private val row3 = Row(3)
  private val row4 = Row(4)

  private val schema = StructType(List(StructField("i", IntegerType)))

  override def beforeAll(): Unit = {
    super.beforeAll()
    jdbcUpdate(s"create table $dbtable(i int primary key)")
  }

  test("Test Write Append") {
    val data: RDD[Row] = sc.makeRDD(List(row3, row4))
    val df = sqlContext.createDataFrame(data, schema)

    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .mode("append")
      .save()

    testTiDBSelect(Seq(row3, row4))
  }
}
