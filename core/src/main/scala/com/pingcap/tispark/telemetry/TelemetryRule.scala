/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.telemetry

import org.apache.spark.sql.{SparkSession, TiExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture

/**
 * A telemetry rule injected into Spark.
 * Telemetry message will be sent at here.
 *
 * @param sparkSession
 */
case class TelemetryRule(sparkSession: SparkSession) extends (LogicalPlan => Unit) {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  val task: Runnable = () => {
    try {
      val telemetry = new Telemetry
      val teleMsg = new TeleMsg(sparkSession)
      telemetry.report(teleMsg)
    } catch {
      case e: Throwable =>
        logger.info("Failed to send telemetry message. " + e.getMessage)
    } finally {
      logger.info("Telemetry done")
    }
  }

  /**
   *  Telemetry message should only be sent once when a Spark application injects rule at startup.
   *  Don't set telemetry task in apply(). If set like that, telemetry will be sent every time the TelemetryRule
   *  is executed.
   */
  if (TiExtensions.telemetryEnable(sparkSession)) {
    CompletableFuture.runAsync(task)
  }

  override def apply(plan: LogicalPlan): Unit = plan
}
