/*
 *
 * Copyright 2018 PingCAP, Inc.
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

import java.io.File

import org.apache.spark.sql.BaseTiSparkTest
import org.apache.spark.sql.catalyst.util.resourceToString

import scala.collection.mutable

class TPCDSQuerySuite extends BaseTiSparkTest {
  private val tpcdsDirectory = getClass.getResource("/tpcds-sql").getPath
  private val tpcdsQueries = getListOfFiles(tpcdsDirectory)

  private def getListOfFiles(dir: String): List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).map(_.getName.stripSuffix(".sql")).toList
    } else {
      List[String]()
    }
  }

  private def run(queries: List[String], numRows: Int = 1, timeout: Int = 0): Unit =
    try {
      // set broadcast threshold to -1 so it will not oom
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
      setCurrentDatabase(tpcdsDBName)
      val succeeded = mutable.ArrayBuffer.empty[String]
      queries.foreach { q =>
        println(s"Query: $q")
        val start = System.currentTimeMillis()
        // We do not use statistic information here due to conflict of netty versions when physical plan has broadcast nodes.
        val queryString = resourceToString(
          s"tpcds-sql/$q.sql",
          classLoader = Thread.currentThread().getContextClassLoader)
        val df = spark.sql(queryString)
        var failed = false
        val jobGroup = s"benchmark $q"
        val t = new Thread("query runner") {
          override def run(): Unit =
            try {
              sqlContext.sparkContext.setJobGroup(jobGroup, jobGroup, interruptOnCancel = true)
              df.show(numRows)
            } catch {
              case e: Exception =>
                println("Failed to run: " + e)
                failed = true
            }
        }
        t.setDaemon(true)
        t.start()
        t.join(timeout)
        if (t.isAlive) {
          println(s"Timeout after $timeout seconds")
          sqlContext.sparkContext.cancelJobGroup(jobGroup)
          t.interrupt()
        } else {
          if (!failed) {
            succeeded += q
            println(s"   Took: ${System.currentTimeMillis() - start} ms")
            println("------------------------------------------------------------------")
          }
        }

        queryViaTiSpark(queryString)
        println(s"TiSpark finished $q")
      }
    } catch {
      case e: Throwable =>
        println(s"TiSpark failed to run TPCDS")
        fail(e)
    }

  test("TPCDS Test") {
    if (runTPCDS) {
      run(tpcdsQueries)
    }
  }
}
