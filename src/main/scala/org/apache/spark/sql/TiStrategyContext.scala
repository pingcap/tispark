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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.types.StructType


class TiStrategyContext {
  var limit: Int = _
  var filters: Seq[Expression] = _
  var projections: Seq[NamedExpression] = _
  var aggregations: Seq[NamedExpression] = _
  var groupingExpressions: Seq[Expression] = _
  private var _output: Seq[Attribute] = Nil

  def output_=(planOutput: Seq[Attribute]) = {
    if (_output.isEmpty) _output = planOutput
  }

  def output = _output

  def schema: StructType = {
    if (output.isEmpty)
      null
    else
      StructType.fromAttributes(output)
  }
}
