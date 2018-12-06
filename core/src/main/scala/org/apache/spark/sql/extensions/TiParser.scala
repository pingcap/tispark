package org.apache.spark.sql.extensions

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
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
  private lazy val internal = new SparkSqlParser(sparkSession.sqlContext.conf)

  private def qualifyTableIdentifierInternal(tableIdentifier: TableIdentifier): TableIdentifier =
    TableIdentifier(
      tableIdentifier.table,
      Some(tableIdentifier.database.getOrElse(tiContext.tiCatalog.getCurrentDatabase))
    )

  /**
   * Determines whether a table specified by tableIdentifier is
   * needs to be qualified. This is used for TiSpark to transform
   * plans and decides whether a relation should be resolved or parsed.
   *
   * @param tableIdentifier tableIdentifier
   * @return whether it needs qualifying
   */
  private def needQualify(tableIdentifier: TableIdentifier) =
    tableIdentifier.database.isEmpty && tiContext.sessionCatalog
      .getTempView(tableIdentifier.table)
      .isEmpty

  /**
   * WAR to lead Spark to consider this relation being on local files.
   * Otherwise Spark will lookup this relation in his session catalog.
   * See [[org.apache.spark.sql.catalyst.analysis.Analyzer.ResolveRelations.resolveRelation]] for detail.
   */
  private val qualifyTableIdentifier: PartialFunction[LogicalPlan, LogicalPlan] = {
    case r @ UnresolvedRelation(tableIdentifier) if needQualify(tableIdentifier) =>
      r.copy(qualifyTableIdentifierInternal(tableIdentifier))
    case i @ InsertIntoTable(r @ UnresolvedRelation(tableIdentifier), _, _, _, _)
        // When getting temp view, we leverage legacy catalog.
        if tableIdentifier.database.isEmpty && tiContext.sessionCatalog
          .getTempView(tableIdentifier.table)
          .isEmpty =>
      i.copy(r.copy(qualifyTableIdentifierInternal(tableIdentifier)))
    case f @ Filter(condition, _) =>
      f.copy(
        condition = condition.transform {
          case e @ Exists(plan, _, _) => e.copy(plan = plan.transform(qualifyTableIdentifier))
          case ls @ ListQuery(plan, _, _, _) =>
            ls.copy(plan = plan.transform(qualifyTableIdentifier))
          case s @ ScalarSubquery(plan, _, _) =>
            s.copy(plan = plan.transform(qualifyTableIdentifier))
        }
      )
    case p @ Project(projectList, _) =>
      p.copy(
        projectList = projectList.map(_.transform {
          case s @ ScalarSubquery(plan, _, _) =>
            s.copy(plan = plan.transform(qualifyTableIdentifier))
        }.asInstanceOf[NamedExpression])
      )
    case w @ With(_, cteRelations) =>
      w.copy(
        cteRelations = cteRelations
          .map(p => (p._1, p._2.transform(qualifyTableIdentifier).asInstanceOf[SubqueryAlias]))
      )
    case cv @ CreateViewCommand(_, _, _, _, _, child, _, _, _) =>
      cv.copy(child = child.transform(qualifyTableIdentifier))
    case e @ ExplainCommand(logicalPlan, _, _, _) =>
      e.copy(logicalPlan = logicalPlan.transform(qualifyTableIdentifier))
    case c @ CacheTableCommand(tableIdentifier, plan, _)
        if plan.isEmpty && needQualify(tableIdentifier) =>
      // Caching an unqualified catalog table.
      c.copy(qualifyTableIdentifierInternal(tableIdentifier))
    case c @ CacheTableCommand(_, plan, _) if plan.isDefined =>
      c.copy(plan = Some(plan.get.transform(qualifyTableIdentifier)))
    case u @ UncacheTableCommand(tableIdentifier, _) if needQualify(tableIdentifier) =>
      // Uncaching an unqualified catalog table.
      u.copy(qualifyTableIdentifierInternal(tableIdentifier))
  }

  override def parsePlan(sqlText: String): LogicalPlan =
    internal.parsePlan(sqlText).transform(qualifyTableIdentifier)

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
