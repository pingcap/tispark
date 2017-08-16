/*
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
 */

package org.apache.spark.sql.hive.thriftserver

import org.apache.commons.logging.LogFactory
import org.apache.hive.service.server.HiveServer2
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2._
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab
import org.apache.spark.util.{ShutdownHookManager, Utils}


object TiHiveThriftServer2 extends Logging {
  var LOG = LogFactory.getLog(classOf[HiveServer2])
  var uiTab: Option[ThriftServerTab] = None
  var listener: HiveThriftServer2Listener = _

  /**
    * :: DeveloperApi ::
    * Starts a new thrift server with the given context.
    */
  @DeveloperApi
  def startWithContext(sqlContext: SQLContext): Unit = {
    val server = new HiveThriftServer2(sqlContext)

    val executionHive = HiveUtils.newClientForExecution(
      sqlContext.sparkContext.conf,
      sqlContext.sessionState.newHadoopConf())

    server.init(executionHive.conf)
    server.start()
    listener = new HiveThriftServer2Listener(server, sqlContext.conf)
    sqlContext.sparkContext.addSparkListener(listener)
    uiTab = if (sqlContext.sparkContext.getConf.getBoolean("spark.ui.enabled", true)) {
      Some(new ThriftServerTab(sqlContext.sparkContext))
    } else {
      None
    }
  }

  def main(args: Array[String]) {
    Utils.initDaemon(log)
    val optionsProcessor = new HiveServer2.ServerOptionsProcessor("TiHiveThriftServer2")
    optionsProcessor.parse(args)

    logInfo("Starting SparkContext")
    TiSparkSQLEnv.init()

    ShutdownHookManager.addShutdownHook { () =>
      TiSparkSQLEnv.stop()
      uiTab.foreach(_.detach())
    }

    val executionHive = HiveUtils.newClientForExecution(
      TiSparkSQLEnv.sqlContext.sparkContext.conf,
      TiSparkSQLEnv.sqlContext.sessionState.newHadoopConf())

    try {
      val server = new HiveThriftServer2(TiSparkSQLEnv.sqlContext)
      server.init(executionHive.conf)
      server.start()
      logInfo("TiHiveThriftServer2 started")
      listener = new HiveThriftServer2Listener(server, TiSparkSQLEnv.sqlContext.conf)
      TiSparkSQLEnv.sparkContext.addSparkListener(listener)
      uiTab = if (TiSparkSQLEnv.sparkContext.getConf.getBoolean("spark.ui.enabled", true)) {
        Some(new ThriftServerTab(TiSparkSQLEnv.sparkContext))
      } else {
        None
      }
      // If application was killed before HiveThriftServer2 start successfully then SparkSubmit
      // process can not exit, so check whether if SparkContext was stopped.
      if (TiSparkSQLEnv.sparkContext.stopped.get()) {
        logError("SparkContext has stopped even if HiveServer2 has started, so exit")
        System.exit(-1)
      }
    } catch {
      case e: Exception =>
        logError("Error starting TiHiveThriftServer2", e)
        System.exit(-1)
    }
  }
}
