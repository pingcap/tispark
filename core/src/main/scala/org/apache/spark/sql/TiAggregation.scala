/*
 * Copyright 2019 PingCAP, Inc.
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

package org.apache.spark.sql

import com.pingcap.tispark.TiDBRelation
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation

object TiAggregation {
  type ReturnType =
    (Seq[NamedExpression], Seq[AggregateExpression], Seq[NamedExpression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = TiAggregationImpl.unapply(plan)
}

object TiAggregationProjection {
  type ReturnType = (Seq[Expression], LogicalPlan, TiDBRelation, Seq[NamedExpression])

  def unapply(plan: LogicalPlan): Option[ReturnType] =
    plan match {
      // Only push down aggregates projection when all filters can be applied and
      // all projection expressions are column references
      case PhysicalOperation(
            projects,
            filters,
            rel @ LogicalRelation(source: TiDBRelation, _, _, _))
          if projects.forall(_.isInstanceOf[Attribute]) =>
        Some((filters, rel, source, projects))
      case _ => Option.empty[ReturnType]
    }
}
