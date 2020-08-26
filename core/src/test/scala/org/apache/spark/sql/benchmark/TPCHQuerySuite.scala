/*
 *
 * Copyright 2017 PingCAP, Inc.
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
 *
 */

package org.apache.spark.sql.benchmark

import org.apache.spark.sql.BaseTiSparkTest
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation
import org.apache.spark.sql.execution.{ColumnarCoprocessorRDD, DataSourceScanExec}

class TPCHQuerySuite extends BaseTiSparkTest {
  private val tpchQueries = (1 to 22).map(i => s"q$i")

  tpchQueries.foreach { name =>
    test(name) {
      if (runTPCH) {
        val queryString = resourceToString(
          s"tpch-sql/$name.sql",
          classLoader = Thread.currentThread().getContextClassLoader)
        runTest(queryString)
      }
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    setCurrentDatabase(tpchDBName)
    loadTestData(Seq(tpchDBName))
  }

  override def afterAll(): Unit = {
    try {
      tableNames.foreach(name => spark.sqlContext.dropTempTable(s"${name}_j"))
      spark.sql("show tables").show(false)
    } finally {
      super.afterAll()
    }
  }
}
