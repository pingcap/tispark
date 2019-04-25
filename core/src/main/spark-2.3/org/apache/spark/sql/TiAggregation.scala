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
import org.apache.spark.sql.catalyst.expressions.NamedExpression.newExprId
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, _}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, Divide, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._

object TiAggregation {
  type ReturnType = PhysicalAggregation.ReturnType

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case PhysicalAggregation(groupingExpressions, aggregateExpressions, resultExpressions, child) =>
      // Rewrites all `Average`s into the form of `Divide(Sum / Count)` so that we can push the
      // converted `Sum`s and `Count`s down to TiKV.
      val (averages, averagesEliminated) = aggregateExpressions.partition {
        case AggregateExpression(_: Average, _, _, _) => true
        case _                                        => false
      }

      // An auxiliary map that maps result attribute IDs of all detected `Average`s to corresponding
      // converted `Sum`s and `Count`s.
      val rewriteMap = averages.map {
        case a @ AggregateExpression(Average(ref), _, _, _) =>
          // We need to do a type promotion on Sum(Long) to avoid LongType overflow in Average rewrite
          // scenarios to stay consistent with original spark's Average behaviour
          val sum = if (ref.dataType.eq(LongType)) PromotedSum(ref) else Sum(ref)
          a.resultAttribute -> Seq(
            a.copy(aggregateFunction = sum, resultId = newExprId),
            a.copy(aggregateFunction = Count(ref), resultId = newExprId)
          )
      }.toMap

      val rewrite: PartialFunction[Expression, Expression] = rewriteMap.map {
        case (ref, Seq(sum, count)) =>
          val castedSum = Cast(sum.resultAttribute, DoubleType)
          val castedCount = Cast(count.resultAttribute, DoubleType)
          val division = Cast(Divide(castedSum, castedCount), ref.dataType)
          (ref: Expression) -> Alias(division, ref.name)(exprId = ref.exprId)
      }

      val rewrittenResultExpressions = resultExpressions
        .map { _ transform rewrite }
        .map { case e: NamedExpression => e }

      val rewrittenAggregateExpressions = {
        val extraSumsAndCounts = rewriteMap.values.reduceOption { _ ++ _ } getOrElse Nil
        (averagesEliminated ++ extraSumsAndCounts).distinct
      }

      Some(groupingExpressions, rewrittenAggregateExpressions, rewrittenResultExpressions, child)

    case _ => Option.empty[ReturnType]
  }
}

object TiAggregationProjection {
  type ReturnType = (Seq[Expression], LogicalPlan, TiDBRelation, Seq[NamedExpression])

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    // Only push down aggregates projection when all filters can be applied and
    // all projection expressions are column references
    case PhysicalOperation(projects, filters, rel @ LogicalRelation(source: TiDBRelation, _, _, _))
        if projects.forall(_.isInstanceOf[Attribute]) =>
      Some((filters, rel, source, projects))
    case _ => Option.empty[ReturnType]
  }
}
