package org.apache.spark.sql.extensions

import com.pingcap.tispark.auth.MySQLPriv
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
  private lazy val tiAuth = tiContext.tiAuthorization
  protected val tiContext: TiContext = getOrCreateTiContext(sparkSession)

  protected def checkForAuth: PartialFunction[LogicalPlan, LogicalPlan] = {
    case sa @ SubqueryAlias(identifier, child) =>
      if (identifier.qualifier.nonEmpty) {
        tiAuth.checkPrivs(
          identifier.qualifier.last,
          identifier.name,
          MySQLPriv.SelectPriv
        )
      }
      sa
    case sd: ShowNamespaces =>
      sd
    case sd: SetCatalogAndNamespace =>
      sd
    case st: ShowTablesCommand =>
      st
    case st: ShowColumnsCommand =>
      st
    case sc @ ShowColumns(
          LogicalRelation(TiDBRelation(_, tableRef, _, _, _), _, _, _),
          _
        ) =>
      sc
    case dt: DescribeTableCommand =>
      dt
    case dt @ DescribeRelation(
          LogicalRelation(TiDBRelation(_, tableRef, _, _, _), _, _, _),
          _,
          _
        ) =>
      dt
    case dc: DescribeColumnCommand =>
      TiDescribeColumnCommand(tiContext, dc)
    case dc @ DescribeColumn(
          LogicalRelation(TiDBRelation(_, tableRef, _, _, _), _, _, _),
          colNameParts,
          isExtended
        ) =>
       dc
    case ct @ CreateTableLikeCommand(target, source, _, _, _, _) =>
      if (target.database.nonEmpty && source.database.nonEmpty) {
        tiAuth.checkPrivs(
          target.database.get,
          target.table,
          MySQLPriv.CreatePriv
        )
        tiAuth.checkPrivs(
          source.database.get,
          source.table,
          MySQLPriv.SelectPriv
        )
      }
      TiCreateTableLikeCommand(tiContext, ct)
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformUp checkForAuth
}
