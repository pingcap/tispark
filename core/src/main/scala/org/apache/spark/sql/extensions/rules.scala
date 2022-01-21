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

import com.pingcap.tispark.utils.ReflectionUtil
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

class TiResolutionRuleFactory(getOrCreateTiContext: SparkSession => TiContext)
  extends (SparkSession => Rule[LogicalPlan]) {
  override def apply(sparkSession: SparkSession): Rule[LogicalPlan] = {
    ReflectionUtil.newTiResolutionRule(getOrCreateTiContext, sparkSession)
  }
}

class TiDDLRuleFactory(getOrCreateTiContext: SparkSession => TiContext)
  extends (SparkSession => Rule[LogicalPlan]) {
  override def apply(sparkSession: SparkSession): Rule[LogicalPlan] = {
    ReflectionUtil.newTiDDLRule(getOrCreateTiContext, sparkSession)
  }
}
