/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.rule

import com.pingcap.tispark.statistics.StatisticsManager
import com.pingcap.tispark.v2.TiDBTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.{SparkSession, TiContext, TiExtensions}
import org.slf4j.LoggerFactory

class TiStatisticsRuleFactory(getOrCreateTiContext: SparkSession => TiContext)
    extends (SparkSession => Rule[LogicalPlan]) {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  override def apply(sparkSession: SparkSession): Rule[LogicalPlan] = {
    TiExtensions.validateCatalog(sparkSession)
    TiStatisticsRule(getOrCreateTiContext)(sparkSession)
  }
}

case class TiStatisticsRule(getOrCreateTiContext: SparkSession => TiContext)(
    sparkSession: SparkSession)
    extends Rule[LogicalPlan] {
  private val tiContext = getOrCreateTiContext(sparkSession)
  private lazy val autoLoad = tiContext.autoLoad

  protected def loadStatistics: PartialFunction[LogicalPlan, LogicalPlan] = {
    case dr@DataSourceV2Relation(
    tiTable@TiDBTable(_, _, _, _, _),
    _,
    _,
    _,
    _) =>
      if (autoLoad) {
        StatisticsManager.loadStatisticsInfo(tiTable.table)
      }
      val sizeInBytes = StatisticsManager.estimateTableSize(tiTable.table)
      tiTable.tableRef.sizeInBytes = sizeInBytes
      dr
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan match {
      case _ =>
        plan transformUp loadStatistics
    }
}
