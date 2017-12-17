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

package com.pingcap.tispark

import com.pingcap.tikv.TiSession
import com.pingcap.tikv.exception.TiClientInternalException
import com.pingcap.tikv.meta.{TiDAGRequest, TiTableInfo, TiTimestamp}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.AggUtils
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.tispark.{TiHandleRDD, TiRDD}
import org.apache.spark.sql.types.StructType

class TiDBRelation(session: TiSession, tableRef: TiTableReference, meta: MetaManager)(
  @transient val sqlContext: SQLContext
) extends BaseRelation {
  val table: TiTableInfo = meta
    .getTable(tableRef.databaseName, tableRef.tableName)
    .getOrElse(throw new TiClientInternalException("Table not exist"))

  override lazy val schema: StructType = TiUtils.getSchemaFromTable(table)

  def logicalPlanToRDD(dagRequest: TiDAGRequest): TiRDD = {
    val ts: TiTimestamp = session.getTimestamp
    dagRequest.setStartTs(ts.getVersion)

    new TiRDD(dagRequest, session.getConf, tableRef, ts, session, sqlContext.sparkSession)
  }

  def logicalPlanToRegionHandlePlan(dagRequest: TiDAGRequest, output: Seq[Attribute]): SparkPlan = {
    val ts: TiTimestamp = session.getTimestamp
    dagRequest.setStartTs(ts.getVersion)
    dagRequest.resolve()
    val tiHandleRDD =
      new TiHandleRDD(dagRequest, session.getConf, tableRef, ts, session, sqlContext.sparkSession)
    val handlePlan = HandleRDDExec(tiHandleRDD)
    val aggFunc = CollectSet(handlePlan.attributeRef.last)

    val aggExpr = AggregateExpression(
      aggFunc,
      Complete,
      isDistinct = true,
      NamedExpression.newExprId
    )

    val sortAgg = AggUtils
      .planAggregateWithoutPartial(
        Seq(handlePlan.attributeRef.head),
        Seq(aggExpr),
        Seq(handlePlan.output.head, aggExpr.resultAttribute),
        handlePlan
      )
      .head

    RegionTaskExec(
      sortAgg,
      output,
      dagRequest,
      session.getConf,
      session.getTimestamp,
      session,
      sqlContext.sparkSession
    )
  }
}
