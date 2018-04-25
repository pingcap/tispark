/*
 * Copyright 2017 PingCAP, Inc.
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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{And, EqualNullSafe, EqualTo, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Provides helper methods for comparing plans.
 */
abstract class PlanTest extends SparkFunSuite with PredicateHelper {

  /**
   * Normalizes plans:
   * - Filter the filter conditions that appear in a plan. For instance,
   *   ((expr 1 && expr 2) && expr 3), (expr 1 && expr 2 && expr 3), (expr 3 && (expr 1 && expr 2)
   *   etc., will all now be equivalent.
   * - Sample the seed will replaced by 0L.
   * - Join conditions will be resorted by hashCode.
   */
  private def normalizePlan(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case filter @ Filter(condition: Expression, child: LogicalPlan) =>
        Filter(
          splitConjunctivePredicates(condition)
            .map(rewriteEqual)
            .sortBy(_.hashCode())
            .reduce(And),
          child
        )
      case sample: Sample =>
        sample.copy(seed = 0L)(true)
      case join @ Join(left, right, joinType, condition) if condition.isDefined =>
        val newCondition =
          splitConjunctivePredicates(condition.get)
            .map(rewriteEqual)
            .sortBy(_.hashCode())
            .reduce(And)
        Join(left, right, joinType, Some(newCondition))
    }
  }

  /**
   * Rewrite [[EqualTo]] and [[EqualNullSafe]] operator to keep order. The following cases will be
   * equivalent:
   * 1. (a = b), (b = a);
   * 2. (a <=> b), (b <=> a).
   */
  private def rewriteEqual(condition: Expression): Expression = condition match {
    case eq @ EqualTo(l: Expression, r: Expression) =>
      Seq(l, r).sortBy(_.hashCode()).reduce(EqualTo)
    case eq @ EqualNullSafe(l: Expression, r: Expression) =>
      Seq(l, r).sortBy(_.hashCode()).reduce(EqualNullSafe)
    case _ => condition // Don't reorder.
  }
}
