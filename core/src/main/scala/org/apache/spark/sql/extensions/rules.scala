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

import com.pingcap.tikv.meta.{TiPartitionDef, TiPartitionInfo}
import com.pingcap.tispark.statistics.StatisticsManager
import com.pingcap.tispark.{MetaManager, PartitionPruningRule, TiDBRelation, TiTableReference}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.TiSessionCatalog
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{AnalysisException, _}

import scala.collection.mutable.ListBuffer

case class TiResolutionRule(getOrCreateTiContext: SparkSession => TiContext)(
  sparkSession: SparkSession
) extends Rule[LogicalPlan] {
  protected lazy val tiContext: TiContext = getOrCreateTiContext(sparkSession)
  private lazy val autoLoad = tiContext.autoLoad
  protected lazy val meta: MetaManager = tiContext.meta
  private lazy val tiCatalog = tiContext.tiCatalog
  private lazy val tiSession = tiContext.tiSession
  private lazy val sqlContext = tiContext.sqlContext

  private def getDatabaseFromIdentifier(tableIdentifier: TableIdentifier): String =
    tableIdentifier.database.getOrElse(tiCatalog.getCurrentDatabase)

  protected val resolveTiDBRelation: (String, String) => TiDBRelation =
    (dbName: String, tableName: String) => {
      val table = meta.getTable(dbName, tableName)
      if (table.isEmpty) {
        throw new AnalysisException(s"Table or view '$tableName' not found in database '$dbName'")
      }
      if (autoLoad) {
        StatisticsManager.loadStatisticsInfo(table.get)
      }
      val sizeInBytes = StatisticsManager.estimateTableSize(table.get)
      TiDBRelation(
        tiSession,
        TiTableReference(dbName, tableName, sizeInBytes),
        meta,
        generatePartitionExpr(table.get.getPartitionInfo, table.get.isPartitionEnabled)
      )(sqlContext)
    }

  // This function will parse partition definitions into Spark's Expression.
  // If we have a partition info such as
  // partition by year(`purchased`)
  // p0 less than 1990
  // p1 less than 2000,
  // we will get
  // p0 year(`purchased`) < 1990
  // p1 year(`purchased`) >= 1990 and year(`purchased`) < 2000
  private def generatePartitionExpr(pInfo: TiPartitionInfo,
                                    partitioned: Boolean): PartitionPruningRule = {
    if (!partitioned) return null
    val parser = sparkSession.sessionState.sqlParser
    val partitionExprs: ListBuffer[Expression] = ListBuffer()
    val locateExprs: ListBuffer[Expression] = ListBuffer()
    var previousPDef: TiPartitionDef = null
    var columnName = ""
    for (i <- 0 to pInfo.getDefs.size() - 1) {
      val sb: StringBuilder = new StringBuilder
      val pDef: TiPartitionDef = pInfo.getDefs().get(i)
      val lessThan = pDef.getLessThan().get(0)
      if (lessThan == "MAXVALUE") {
        sb.append("true")
      } else {
        sb.append(s"((${pInfo.getExpr}) < (${pDef.getLessThan().get(0)}))")
      }

      locateExprs :+ resolveUnresolvedFnInExpr(parser.parseExpression(sb.toString())).getOrElse(
        throw new IllegalStateException(
          s"${sb.toString()} cannot" +
            s"be parsed."
        )
      )
      if (i > 0) {
        sb.append(s" and ((${pInfo.getExpr}) >= (${previousPDef.getLessThan().get(0)}))")
      } else {
        // NULL will locate in the first partition, so its expression is
        // (expr < value or expr is null).
//        sb.append(s" or ((${pInfo.getExpr}) is null)")

        // Since we build a expression string and let SparkSQLParser parses
        // such expression. The expected attribute is always [[UnresolvedAttribute]],
        // so directly getting value from Option seems safe.
        columnName = resolveUnresolvedFnInExpr(parser.parseExpression(pInfo.getExpr)).get
          .find((e) => e.isInstanceOf[UnresolvedAttribute])
          .get
          .asInstanceOf[UnresolvedAttribute]
          .name
      }

      // parse entire partition expression.
      val pExpr = resolveUnresolvedFnInExpr(parser.parseExpression(sb.toString()))
      partitionExprs += pExpr.getOrElse(
        throw new IllegalStateException(
          s"${sb.toString()} cannot" +
            s"be parsed."
        )
      )
      previousPDef = pDef
    }
    new PartitionPruningRule(
      partitionExprs.toList,
      resolveUnresolvedFnInExpr(parser.parseExpression(pInfo.getExpr)).getOrElse(
        throw new IllegalStateException(
          s"${pInfo.getExpr} cannot" +
            s"be parsed."
        )
      ),
      columnName
    )
  }

  private def resolveUnresolvedFnInExpr(expr: Expression): Option[Expression] =
    Option(expr transform {
      case UnresolvedFunction(funcID, children, _) => {
        tiCatalog.lookupFunction(funcID, children)
      }
    })

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case rel @ UnresolvedRelation(tableIdentifier) =>
      val dbName = getDatabaseFromIdentifier(tableIdentifier)
      if (!tiCatalog.isTemporaryTable(tableIdentifier) && tiCatalog
            .catalogOf(Option.apply(dbName))
            .exists(_.isInstanceOf[TiSessionCatalog])) {
        LogicalRelation(resolveTiDBRelation(dbName, tableIdentifier.table))
      } else {
        rel
      }
  }
}

case class TiDDLRule(getOrCreateTiContext: SparkSession => TiContext)(sparkSession: SparkSession)
    extends Rule[LogicalPlan] {
  protected lazy val tiContext: TiContext = getOrCreateTiContext(sparkSession)

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // TODO: support other commands that may concern TiSpark catalog.
    case sd: ShowDatabasesCommand =>
      TiShowDatabasesCommand(tiContext, sd)
    case sd: SetDatabaseCommand =>
      TiSetDatabaseCommand(tiContext, sd)
    case st: ShowTablesCommand =>
      TiShowTablesCommand(tiContext, st)
    case dt: DescribeTableCommand =>
      TiDescribeTablesCommand(tiContext, dt)
  }
}
