package org.apache.spark.sql.extensions
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Exists, Expression, ListQuery, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{SparkSession, TiContext}

case class TiParser(getOrCreateTiContext: SparkSession => TiContext)(sparkSession: SparkSession,
                                                                     delegate: ParserInterface)
    extends ParserInterface {
  private lazy val tiContext = getOrCreateTiContext(sparkSession)

  private lazy val internal = new SparkSqlParser(sparkSession.sqlContext.conf)

  /**
   * WAR to lead Spark to consider this relation being on local files.
   * Otherwise Spark will lookup this relation in his session catalog.
   * See [[org.apache.spark.sql.catalyst.analysis.Analyzer.ResolveRelations.resolveRelation]] for detail.
   */
  private val qualifyTableIdentifier: PartialFunction[LogicalPlan, LogicalPlan] = {
    case r @ UnresolvedRelation(tableIdentifier)
        if tiContext.tiCatalog.tableExists(
          TableIdentifier(
            tableIdentifier.table,
            Some(tableIdentifier.database.getOrElse(tiContext.tiCatalog.getCurrentDatabase))
          )
        ) =>
      r.copy(
        TableIdentifier(
          tableIdentifier.table,
          Some(tableIdentifier.database.getOrElse(tiContext.tiCatalog.getCurrentDatabase))
        )
      )
    case f @ Filter(condition, _) =>
      f.copy(condition = condition.transform {
        case e @ Exists(plan, _, _)         => e.copy(plan = plan.transform(qualifyTableIdentifier))
        case ls @ ListQuery(plan, _, _, _)  => ls.copy(plan = plan.transform(qualifyTableIdentifier))
        case s @ ScalarSubquery(plan, _, _) => s.copy(plan = plan.transform(qualifyTableIdentifier))
      })
    case p @ Project(projectList, _) =>
      p.copy(projectList = projectList.map(_.transform {
        case s @ ScalarSubquery(plan, _, _) => s.copy(plan = plan.transform(qualifyTableIdentifier))
      }.asInstanceOf[NamedExpression]))
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
