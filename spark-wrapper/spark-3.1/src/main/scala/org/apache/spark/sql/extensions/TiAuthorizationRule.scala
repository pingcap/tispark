package org.apache.spark.sql.extensions

import com.pingcap.tispark.auth.TiAuthorization
import com.pingcap.tispark.{MetaManager, TiDBRelation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{SparkSession, TiContext}

case class TiAuthorizationRule(getOrCreateTiContext: SparkSession => TiContext)(
    sparkSession: SparkSession
) extends Rule[LogicalPlan] {

  protected lazy val meta: MetaManager = tiContext.meta
  protected val tiContext: TiContext = getOrCreateTiContext(sparkSession)
  private lazy val tiAuthorization: TiAuthorization = tiContext.tiAuthorization

  protected def checkForAuth: PartialFunction[LogicalPlan, LogicalPlan] = {
    case sa @ SubqueryAlias(identifier, child) =>
      if (identifier.qualifier.nonEmpty) {
        TiAuthorization.authorizeForSelect(
          identifier.name,
          identifier.qualifier.last,
          tiAuthorization
        )
      }
      sa
    case sd: ShowNamespaces =>
      sd
    case sd @ SetCatalogAndNamespace(catalogManager, catalogName, namespace) =>
      namespace.get.foreach(
        TiAuthorization.authorizeForSetDatabase(_, tiAuthorization)
      )
      sd
    case st: ShowTablesCommand =>
      st
    case st @ ShowColumnsCommand(databaseName, tableName) =>
      TiAuthorization.authorizeForDescribeTable(
        tableName.table,
        tiContext.getDatabaseFromOption(databaseName),
        tiAuthorization
      )
      st
    case sc @ ShowColumns(
          LogicalRelation(TiDBRelation(_, tableRef, _, _, _), _, _, _),
          _
        ) =>
      TiAuthorization.authorizeForDescribeTable(
        tableRef.tableName,
        tableRef.databaseName,
        tiAuthorization
      )
      sc
    case dt @ DescribeTableCommand(table, _, _) =>
      TiAuthorization.authorizeForDescribeTable(
        table.table,
        tiContext.getDatabaseFromOption(table.database),
        tiAuthorization
      )
      dt
    case dt @ DescribeRelation(
          LogicalRelation(TiDBRelation(_, tableRef, _, _, _), _, _, _),
          _,
          _
        ) =>
      TiAuthorization.authorizeForDescribeTable(
        tableRef.tableName,
        tableRef.databaseName,
        tiAuthorization
      )
      dt
    case dc @ DescribeColumnCommand(table, _, _) =>
      TiAuthorization.authorizeForDescribeTable(
        table.table,
        tiContext.getDatabaseFromOption(table.database),
        tiAuthorization
      )
      dc
    case dc @ DescribeColumn(
          LogicalRelation(TiDBRelation(_, tableRef, _, _, _), _, _, _),
          colNameParts,
          isExtended
        ) =>
      TiAuthorization.authorizeForDescribeTable(
        tableRef.tableName,
        tableRef.databaseName,
        tiAuthorization
      )
      dc
    case ct @ CreateTableLikeCommand(target, source, _, _, _, _) =>
      TiAuthorization.authorizeForCreateTableLike(
        tiContext.getDatabaseFromOption(target.database),
        target.table,
        tiContext.getDatabaseFromOption(source.database),
        source.table,
        tiContext.tiAuthorization
      )
      TiCreateTableLikeCommand(tiContext, ct)
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformUp checkForAuth
}
