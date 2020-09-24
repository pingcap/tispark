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

import java.util

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command.{
  CacheTableCommand,
  CreateViewCommand,
  ExplainCommand,
  UncacheTableCommand
}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{SparkSession, TiContext, TiExtensions}

class TiParserFactory(getOrCreateTiContext: SparkSession => TiContext)
    extends ((SparkSession, ParserInterface) => ParserInterface) {
  override def apply(
      sparkSession: SparkSession,
      parserInterface: ParserInterface): ParserInterface = {
    if (TiExtensions.catalogPluginMode(sparkSession)) {
      parserInterface
    } else {
      TiParser(getOrCreateTiContext)(sparkSession, parserInterface)
    }
  }
}

case class TiParser(getOrCreateTiContext: SparkSession => TiContext)(
    sparkSession: SparkSession,
    delegate: ParserInterface)
    extends ParserInterface {
  private lazy val tiContext = getOrCreateTiContext(sparkSession)
  private lazy val internal = new SparkSqlParser(sparkSession.sqlContext.conf)

  private val cteTableNames = new ThreadLocal[java.util.Set[String]] {
    override def initialValue(): util.Set[String] = new util.HashSet[String]()
  }

  /**
   * WAR to lead Spark to consider this relation being on local files.
   * Otherwise Spark will lookup this relation in his session catalog.
   * CHECK Spark [[org.apache.spark.sql.catalyst.analysis.Analyzer.ResolveRelations]] for details.
   */
  private val qualifyTableIdentifier: PartialFunction[LogicalPlan, LogicalPlan] = {
    case r @ UnresolvedRelation(tableIdentifier) if needQualify(tableIdentifier) =>
      r.copy(qualifyTableIdentifierInternal(tableIdentifier))
    case i @ InsertIntoStatement(r @ UnresolvedRelation(tableIdentifier), _, _, _, _)
        if needQualify(tableIdentifier) =>
      // When getting temp view, we leverage legacy catalog.
      i.copy(r.copy(qualifyTableIdentifierInternal(tableIdentifier)))
    case w @ With(_, cteRelations) =>
      for (x <- cteRelations) {
        cteTableNames.get().add(x._1.toLowerCase())
      }
      w.copy(cteRelations = cteRelations
        .map(p => (p._1, p._2.transform(qualifyTableIdentifier).asInstanceOf[SubqueryAlias])))
    case cv @ CreateViewCommand(_, _, _, _, _, child, _, _, _) =>
      cv.copy(child = child transform qualifyTableIdentifier)
    case e @ ExplainCommand(plan, _) =>
      e.copy(logicalPlan = plan transform qualifyTableIdentifier)
    case c @ CacheTableCommand(tableIdentifier, plan, _, _)
        if plan.isEmpty && needQualify(tableIdentifier) =>
      // Caching an unqualified catalog table.
      c.copy(qualifyTableIdentifierInternal(tableIdentifier))
    case c @ CacheTableCommand(_, plan, _, _) if plan.isDefined =>
      c.copy(plan = Some(plan.get transform qualifyTableIdentifier))
    case u @ UncacheTableCommand(tableIdentifier, _) if needQualify(tableIdentifier) =>
      // Uncaching an unqualified catalog table.
      u.copy(qualifyTableIdentifierInternal(tableIdentifier))
    case logicalPlan =>
      logicalPlan transformExpressionsUp {
        case s: SubqueryExpression =>
          val cteNamesBeforeSubQuery = new util.HashSet[String]()
          cteNamesBeforeSubQuery.addAll(cteTableNames.get())
          val newPlan = s.withNewPlan(s.plan transform qualifyTableIdentifier)
          // cte table names in the subquery should not been seen outside subquey
          cteTableNames.get().clear()
          cteTableNames.get().addAll(cteNamesBeforeSubQuery)
          newPlan
      }
  }

  override def parsePlan(sqlText: String): LogicalPlan = {
    val plan = internal.parsePlan(sqlText)
    cteTableNames.get().clear()
    plan.transform(qualifyTableIdentifier)
  }

  override def parseExpression(sqlText: String): Expression =
    internal.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    internal.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    internal.parseFunctionIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    internal.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    internal.parseDataType(sqlText)

  private def qualifyTableIdentifierInternal(tableIdentifier: Seq[String]): Seq[String] = {
    if (tableIdentifier.size == 1) {
      tiContext.tiCatalog.getCurrentDatabase :: tableIdentifier.toList
    } else {
      tableIdentifier
    }
  }

  private def qualifyTableIdentifierInternal(
      tableIdentifier: TableIdentifier): TableIdentifier = {
    TableIdentifier(
      tableIdentifier.table,
      Some(tableIdentifier.database.getOrElse(tiContext.tiCatalog.getCurrentDatabase)))
  }

  private def needQualify(tableIdentifier: Seq[String]): Boolean = {
    tableIdentifier.size == 1 && tiContext.sessionCatalog
      .getTempView(tableIdentifier.head)
      .isEmpty && !cteTableNames.get().contains(tableIdentifier.head.toLowerCase())
  }

  /**
   * Determines whether a table specified by tableIdentifier is
   * needs to be qualified. This is used for TiSpark to transform
   * plans and decides whether a relation should be resolved or parsed.
   *
   * @param tableIdentifier tableIdentifier
   * @return whether it needs qualifying
   */
  private def needQualify(tableIdentifier: TableIdentifier): Boolean = {
    tableIdentifier.database.isEmpty && tiContext.sessionCatalog
      .getTempView(tableIdentifier.table)
      .isEmpty
  }

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    internal.parseMultipartIdentifier(sqlText)

  @scala.throws[ParseException]("Text cannot be parsed to a DataType")
  override def parseRawDataType(sqlText: String): DataType = ???
}
