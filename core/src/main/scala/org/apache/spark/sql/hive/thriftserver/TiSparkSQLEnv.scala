/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver

import java.io.PrintStream

import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.{HiveSessionState, HiveUtils}
import org.apache.spark.sql.{SQLContext, TiSparkSession}
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}

/** A singleton object for the master program. The slaves should not access this. */
private[hive] object TiSparkSQLEnv extends Logging {
  logDebug("Initializing TiSparkSQLEnv")

  var sqlContext: SQLContext = _
  var sparkContext: SparkContext = _

  def init() {
    if (sqlContext == null) {
      val sparkConf = new SparkConf(loadDefaults = true)
      // If user doesn't specify the appName, we want to get [SparkSQL::localHostName] instead of
      // the default appName [TiSparkSQLCLIDriver] in cli or beeline.
      val maybeAppName = sparkConf
        .getOption("spark.app.name")
        .filterNot(_ == classOf[SparkSQLCLIDriver].getName)

      sparkConf
        .setAppName(
          maybeAppName.getOrElse(s"SparkSQL::${Utils.localHostName()}")
        )
        .set("spark.tispark.db_prefix", "tidb")

      // Injection point for TiSparkSession
      val sparkSession = TiSparkSession
        .builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
      sparkContext = sparkSession.sparkContext
      sqlContext = sparkSession.sqlContext

      val sessionState =
        sparkSession.sessionState.asInstanceOf[HiveSessionState]
      sessionState.metadataHive.setOut(
        new PrintStream(System.out, true, "UTF-8")
      )
      sessionState.metadataHive.setInfo(
        new PrintStream(System.err, true, "UTF-8")
      )
      sessionState.metadataHive.setError(
        new PrintStream(System.err, true, "UTF-8")
      )
      sparkSession.conf
        .set("spark.sql.hive.version", HiveUtils.hiveExecutionVersion)
    }
  }

  /** Cleans up and shuts down the Spark SQL environments. */
  def stop() {
    logDebug("Shutting down Spark SQL Environment")
    // Stop the SparkContext
    if (TiSparkSQLEnv.sparkContext != null) {
      sparkContext.stop()
      sparkContext = null
      sqlContext = null
    }
  }
}
