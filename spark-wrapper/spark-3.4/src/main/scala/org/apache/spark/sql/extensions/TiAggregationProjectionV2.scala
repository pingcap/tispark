/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.extensions

import com.pingcap.tispark.v2.TiDBTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.{
  DataSourceV2Relation,
  DataSourceV2ScanRelation
}

/**
 * I'm afraid that the same name with the object under the spark-wrapper/spark3.0 will lead to some problems.
 * Although the same name will pass the integration test
 */
object TiAggregationProjectionV2 {
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
              _,
              _,
              _)) if projects.forall(_.isInstanceOf[Attribute]) =>
        Some((filters, rel, source, projects))
      case _ => Option.empty[ReturnType]
    }
}
