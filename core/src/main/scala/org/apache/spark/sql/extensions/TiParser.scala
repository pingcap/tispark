package org.apache.spark.sql.extensions

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.TiSessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Exists, Expression, ListQuery, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command.{CacheTableCommand, CreateViewCommand, ExplainCommand, UncacheTableCommand}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{SparkSession, TiContext}

case class TiParser(getOrCreateTiContext: SparkSession => TiContext)(sparkSession: SparkSession,
                                                                     delegate: ParserInterface)
    extends ParserInterface {
  private lazy val tiContext = getOrCreateTiContext(sparkSession)
  private lazy val tiCatalog = tiContext.tiCatalog
  private lazy val internal = new SparkSqlParser(sparkSession.sqlContext.conf)

  private def qualifyTableIdentifierInternal(tableIdentifier: TableIdentifier): TableIdentifier =
    TableIdentifier(
      tableIdentifier.table,
      Some(tableIdentifier.database.getOrElse(tiContext.tiCatalog.getCurrentDatabase))
    )

  /**
   * Determines whether a table specified by tableIdentifier is
   * NOT a tempView registered. This is used for TiSpark to transform
   * plans and decides whether a relation should be resolved or parsed.
   *
   * @param tableIdentifier tableIdentifier
   * @return whether it is not a tempView
   */
  private def notTempView(tableIdentifier: TableIdentifier) =
    !tiCatalog.isTemporaryTable(tableIdentifier)

  /**
   * WAR to lead Spark to consider this relation being on local files.
   * Otherwise Spark will lookup this relation in his session catalog.
   * See [[org.apache.spark.sql.catalyst.analysis.Analyzer.ResolveRelations.resolveRelation]] for detail.
   *
   * Here we use transformUp when transforming logicalPlans because We should first
   * deal with the leaf nodes, and a bottom-to-top regression is needed.
   */
  private val qualifyTableIdentifier: PartialFunction[LogicalPlan, LogicalPlan] = {
    case r @ UnresolvedRelation(tableIdentifier)
        if tiCatalog
          .catalogOf(tableIdentifier.database)
          .exists(_.isInstanceOf[TiSessionCatalog]) && notTempView(tableIdentifier) =>
      // Use SubqueryAlias so that projects and joins can correctly resolve
      // UnresolvedAttributes in JoinConditions, Projects, Filters, etc.
      SubqueryAlias.apply(
        tableIdentifier.table,
        child = r.copy(qualifyTableIdentifierInternal(tableIdentifier))
      )
    case f @ Filter(condition, _) =>
      f.copy(
        condition = condition.transformUp {
          case e @ Exists(plan, _, _) => e.copy(plan = plan.transformUp(qualifyTableIdentifier))
          case ls @ ListQuery(plan, _, _, _) =>
            ls.copy(plan = plan.transformUp(qualifyTableIdentifier))
          case s @ ScalarSubquery(plan, _, _) =>
            s.copy(plan = plan.transformUp(qualifyTableIdentifier))
        }
      )
    case p @ Project(projectList, _) =>
      p.copy(
        projectList = projectList.map(_.transformUp {
          case s @ ScalarSubquery(plan, _, _) =>
            s.copy(plan = plan.transformUp(qualifyTableIdentifier))
        }.asInstanceOf[NamedExpression])
      )
    case w @ With(_, cteRelations) =>
      w.copy(
        cteRelations = cteRelations
          .map(p => (p._1, p._2.transformUp(qualifyTableIdentifier).asInstanceOf[SubqueryAlias]))
      )
    case cv @ CreateViewCommand(_, _, _, _, _, child, _, _, _) =>
      cv.copy(child = child.transformUp(qualifyTableIdentifier))
    case e @ ExplainCommand(logicalPlan, _, _, _) =>
      e.copy(logicalPlan = logicalPlan.transformUp(qualifyTableIdentifier))
    case c @ CacheTableCommand(tableIdentifier, plan, _)
        if plan.isEmpty && notTempView(tableIdentifier) =>
      // Caching an unqualified catalog table.
      c.copy(qualifyTableIdentifierInternal(tableIdentifier))
    case c @ CacheTableCommand(_, plan, _) if plan.isDefined =>
      c.copy(plan = Some(plan.get.transformUp(qualifyTableIdentifier)))
    case u @ UncacheTableCommand(tableIdentifier, _) if notTempView(tableIdentifier) =>
      // Uncaching an unqualified catalog table.
      u.copy(qualifyTableIdentifierInternal(tableIdentifier))
  }

  override def parsePlan(sqlText: String): LogicalPlan =
    internal.parsePlan(sqlText).transformUp(qualifyTableIdentifier)

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
}
