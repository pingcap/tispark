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
  private val tpchQueries = Seq(
    "q1",
    "q2",
    "q3",
    "q4",
    "q5",
    "q6",
    "q7",
    "q8",
    "q9",
    "q10",
    "q11",
    "q12",
    "q13",
    "q14",
    "q15",
    "q16",
    "q17",
    "q18",
    "q19",
    "q20",
    "q21", // May cause OOM if data set is large
    "q22")

  private val tpchTables = Seq(
    "lineitem",
    "orders",
    "customer",
    "nation",
    "customer",
    "partsupp",
    "part",
    "region",
    "supplier")

  private def tiSparkRes(name: String) =
    try {
      setCurrentDatabase(tpchDBName)
      // We do not use statistic information here due to conflict of netty versions when physical plan has broadcast nodes.
      val queryString = resourceToString(
        s"tpch-sql/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)
      spark.sql(queryString).queryExecution.executedPlan.foreach {
        case scan: DataSourceScanExec =>
          scan.relation match {
            case _: JDBCRelation =>
              throw new AssertionError(
                "Coprocessor plan should not use JDBC Scan as data source node!")
            case _ =>
          }
        case _ =>
      }
      val res = queryViaTiSpark(queryString)
      println(s"TiSpark finished $name")
      res
    } catch {
      case e: Throwable =>
        println(s"TiSpark failed $name")
        fail(e)
    }

  private def jdbcRes(name: String) =
    try {
      tpchTables.foreach(createOrReplaceTempView(tpchDBName, _, ""))
      val queryString = resourceToString(
        s"tpch-sql/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)
      spark.sql(queryString).queryExecution.executedPlan.foreach {
        case _: ColumnarCoprocessorRDD =>
          throw new AssertionError("JDBC plan should not use CoprocessorRDD as data source node!")
        case _ =>
      }
      val res = queryViaTiSpark(queryString)
      println(s"Spark JDBC finished $name")
      res
    } catch {
      case e: Throwable =>
        println(s"Spark JDBC failed $name")
        fail(e)
    } finally {
      tpchTables.foreach(spark.sqlContext.dropTempTable)
    }

  tpchQueries.foreach { name =>
    test(name) {
      if (runTPCH) {
        assertResult(tiSparkRes(name))(jdbcRes(name))
      }
    }
  }
}
