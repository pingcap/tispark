/*
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
 */
package org.apache.spark.sql.extensions

import com.pingcap.tispark.auth.TiAuthorization
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import com.pingcap.tispark.utils.ReflectionUtil
import org.apache.spark.sql.{SparkSession, TiContext, TiExtensions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.slf4j.LoggerFactory

class TiAuthRuleFactory(getOrCreateTiContext: SparkSession => TiContext)
  extends (SparkSession => Rule[LogicalPlan]) {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  override def apply(sparkSession: SparkSession): Rule[LogicalPlan] = {
    if (TiExtensions.authEnable(sparkSession)) {
      // set the class loader to Reflection class loader to avoid class not found exception while loading TiCatalog
      logger.info("TiSpark running in auth mode")
      TiAuthorization.enableAuth = true
      ReflectionUtil.newTiAuthRule(getOrCreateTiContext, sparkSession)
    } else {
      TiNopAuthRule(getOrCreateTiContext)(sparkSession)
    }
  }
}

class TiResolutionRuleFactory(getOrCreateTiContext: SparkSession => TiContext)
    extends (SparkSession => Rule[LogicalPlan]) {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  override def apply(sparkSession: SparkSession): Rule[LogicalPlan] = {
    if (TiExtensions.catalogPluginMode(sparkSession)) {
      // set the class loader to Reflection class loader to avoid class not found exception while loading TiCatalog
      logger.info("TiSpark running in catalog plugin mode")
      ReflectionUtil.newTiResolutionRuleV2(getOrCreateTiContext, sparkSession)
    } else {
      ReflectionUtil.newTiResolutionRule(getOrCreateTiContext, sparkSession)
    }
  }
}

class TiDDLRuleFactory(getOrCreateTiContext: SparkSession => TiContext)
    extends (SparkSession => Rule[LogicalPlan]) {
  override def apply(sparkSession: SparkSession): Rule[LogicalPlan] = {
    if (TiExtensions.catalogPluginMode(sparkSession)) {
      TiDDLRuleV2(getOrCreateTiContext)(sparkSession)
    } else {
      ReflectionUtil.newTiDDLRule(getOrCreateTiContext, sparkSession)
    }
  }
}

case class NopCommand(name: String) extends Command {}

case class TiDDLRuleV2(getOrCreateTiContext: SparkSession => TiContext)(
    sparkSession: SparkSession)
    extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

case class TiNopAuthRule(getOrCreateTiContext: SparkSession => TiContext)(
  sparkSession: SparkSession)
  extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan
}
