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
