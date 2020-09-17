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
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class MissingParameterSuite extends BaseBatchWriteTest("test_datasource_missing_parameter") {
  private val row1 = Row(null, "Hello")

  private val schema = StructType(
    List(StructField("i", IntegerType), StructField("s", StringType)))

  test("Missing parameter: database") {
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")

    val caught = intercept[IllegalArgumentException] {
      val rows = row1 :: Nil
      val data: RDD[Row] = sc.makeRDD(rows)
      val df = sqlContext.createDataFrame(data, schema)
      df.write
        .format("tidb")
        .options(tidbOptions)
        .option("table", table)
        .mode("append")
        .save()
    }
    assert(
      caught.getMessage
        .equals("requirement failed: Option 'database' is required."))
  }
}
