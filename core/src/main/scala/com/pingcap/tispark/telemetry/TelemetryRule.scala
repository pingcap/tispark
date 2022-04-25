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
import org.apache.spark.sql.catalyst.rules.Rule
import org.slf4j.LoggerFactory

/**
 * A telemetry rule injected into Spark.
 * Telemetry message will be sent at here.
 *
 * @param sparkSession
 */
case class TelemetryRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  if (TiExtensions.telemetryEnable(sparkSession)) {
    if (TeleMsg.shouldSendMsg) {
      val telemetry = new Telemetry
      telemetry.report(TeleMsg)
      logger.info("Telemetry done")
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan
  }
}
