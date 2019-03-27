/*
 * Copyright 2019 PingCAP, Inc.
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

package org.apache.spark.sql

import com.pingcap.tispark.{TiBatchWrite, TiTableReference}
import org.apache.spark.rdd.RDD
import org.scalatest.Ignore

@Ignore
class TiBatchWriteSuite extends BaseTiSparkSuite {
  test("ti batch write") {
    sql("show databases").show()
    setCurrentDatabase("tpch_test")
    val df = sql("select * from CUSTOMER")
    df.show()

    val rdd: RDD[Row] = df.rdd
    val tableRef: TiTableReference = TiTableReference("tidb_tpch_test", "test")
    val tiContext: TiContext = this.ti
    TiBatchWrite.writeToTiDB(rdd, tableRef, tiContext)
  }
}
