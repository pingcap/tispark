package org.apache.spark.sql.extensions

import com.pingcap.tispark.v2.TiDBTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.{
  DataSourceV2Relation,
  DataSourceV2ScanRelation
}

object TiAggregationProjection {
  type ReturnType = (Seq[Expression], LogicalPlan, TiDBTable, Seq[NamedExpression])

  def unapply(plan: LogicalPlan): Option[ReturnType] =
    plan match {
      // Only push down aggregates projection when all filters can be applied and
      // all projection expressions are column references
      case PhysicalOperation(
            projects,
            filters,
            rel @ DataSourceV2ScanRelation(
              DataSourceV2Relation(source: TiDBTable, _, _, _, _),
              _,
              _)) if projects.forall(_.isInstanceOf[Attribute]) =>
        Some((filters, rel, source, projects))
      case _ => Option.empty[ReturnType]
    }
}
