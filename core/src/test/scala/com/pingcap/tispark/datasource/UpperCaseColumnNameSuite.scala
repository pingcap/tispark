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

class UpperCaseColumnNameSuite
    extends BaseBatchWriteWithoutDropTableTest("test_datasource_uppser_case_column_name") {

  private val row1 = Row(1, 2)

  private val schema = StructType(
    List(StructField("O_ORDERKEY", IntegerType), StructField("O_CUSTKEY", IntegerType)))

  override def beforeAll(): Unit = {
    super.beforeAll()

    jdbcUpdate(s"""
                  |CREATE TABLE $dbtable (O_ORDERKEY INTEGER NOT NULL,
                  |                       O_CUSTKEY INTEGER NOT NULL);
       """.stripMargin)
  }

  test("Test insert upper case column name") {
    val data: RDD[Row] = sc.makeRDD(List(row1))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .mode("append")
      .save()
  }
}
