/*
 *
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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.sql.catalyst.expressions

import com.pingcap.tikv.expression.visitor.{
  ColumnMatcher,
  MetaResolver,
  SupportedExpressionValidator
}
import com.pingcap.tikv.expression.{
  AggregateFunction,
  ByItem,
  ColumnRef,
  Constant,
  ExpressionBlocklist
}
import com.pingcap.tikv.meta.{TiColumnInfo, TiDAGRequest, TiTableInfo}
import com.pingcap.tikv.region.RegionStoreClient.RequestTypes
import com.pingcap.tikv.types.SetType
import com.pingcap.tispark.v2.TiDBTable
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution.TiConverter.fromSparkType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

object TiExprUtils {
  type TiDataType = com.pingcap.tikv.types.DataType
  type TiExpression = com.pingcap.tikv.expression.Expression

  def transformGroupingToTiGrouping(
      expr: Expression,
      meta: TiTableInfo,
      dagRequest: TiDAGRequest): Unit =
    expr match {
      case BasicExpression(keyExpr) =>
        MetaResolver.resolve(keyExpr, meta)
        dagRequest.addGroupByItem(ByItem.create(keyExpr, false))
        // We need to add a `First` function in DAGRequest along with group by
        dagRequest.getFields.asScala
          .filter(ColumnMatcher.`match`(_, keyExpr))
          .foreach(
            (ref: ColumnRef) =>
              dagRequest
                .addAggregate(
                  AggregateFunction
                    .newCall(
                      AggregateFunction.FunctionType.First,
                      ref,
                      meta.getColumn(ref.getName).getType)))
      case _ =>
    }

  def transformAggExprToTiAgg(
      expr: Expression,
      meta: TiTableInfo,
      dagRequest: TiDAGRequest): Any =
    expr match {
      case _: Average =>
        throw new IllegalArgumentException("Should never be here")

      case f: Sum =>
        val arg = BasicExpression.unapply(f.child).get
        addingSumAggToDAgReq(meta, dagRequest, f, arg)

      case f @ PromotedSum(BasicExpression(arg)) =>
        addingSumAggToDAgReq(meta, dagRequest, f, arg)

      case f @ Count(args) if args.lengthCompare(1) == 0 =>
        val tiArg = if (args.head.isInstanceOf[Literal]) {
          val firstColRef = if (meta.hasPrimaryKey) {
            val col = meta.getColumns.asScala.filter(col => col.isPrimaryKey).head
            ColumnRef.create(col.getName, meta)
          } else {
            if (dagRequest.getFields.isEmpty) {
              val firstCol = meta.getColumns.get(0)
              ColumnRef.create(firstCol.getName, meta)
            } else {
              dagRequest.getFields.head
            }
          }
          if (dagRequest.getFields.isEmpty) {
            dagRequest.addRequiredColumn(firstColRef)
          }
          // we need to push down Constant(1) for count(1) or count(*)
          Constant.create(1, null)
        } else {
          args.flatMap(BasicExpression.convertToTiExpr).head
        }
        dagRequest.addAggregate(
          AggregateFunction
            .newCall(AggregateFunction.FunctionType.Count, tiArg, fromSparkType(f.dataType)))

      case _ @Min(BasicExpression(arg)) =>
        MetaResolver.resolve(arg, meta)
        dagRequest
          .addAggregate(
            AggregateFunction
              .newCall(AggregateFunction.FunctionType.Min, arg))

      case _ @Max(BasicExpression(arg)) =>
        MetaResolver.resolve(arg, meta)
        dagRequest
          .addAggregate(
            AggregateFunction
              .newCall(AggregateFunction.FunctionType.Max, arg))

      case _ @First(BasicExpression(arg), _) =>
        MetaResolver.resolve(arg, meta)
        dagRequest
          .addAggregate(
            AggregateFunction
              .newCall(AggregateFunction.FunctionType.First, arg))

      case _ =>
    }

  private def addingSumAggToDAgReq(
      meta: TiTableInfo,
      dagRequest: TiDAGRequest,
      f: DeclarativeAggregate,
      arg: TiExpression) = {
    MetaResolver.resolve(arg, meta)
    dagRequest
      .addAggregate(
        AggregateFunction
          .newCall(AggregateFunction.FunctionType.Sum, arg, fromSparkType(f.dataType)))
  }

  def transformFilter(
      expr: Expression,
      meta: TiTableInfo,
      dagRequest: TiDAGRequest): TiExpression = {
    expr match {
      case BasicExpression(arg) =>
        MetaResolver.resolve(arg, meta)
        arg
    }
  }

  def transformSortOrderToTiOrderBy(
      request: TiDAGRequest,
      sortOrder: Seq[SortOrder],
      meta: TiTableInfo): Unit = {
    val byItems = sortOrder.map { order =>
      {
        val expr = order.child
        val tiExpr = expr match {
          case BasicExpression(arg) => arg
        }
        MetaResolver.resolve(tiExpr, meta)
        ByItem.create(tiExpr, order.direction.sql.equalsIgnoreCase("DESC"))
      }
    }
    byItems.foreach(request.addOrderByItem)
  }

  def transformAttrToColRef(attr: Attribute, meta: TiTableInfo): TiExpression = {
    attr match {
      case BasicExpression(expr) =>
        MetaResolver.resolve(expr, meta)
        expr
    }
  }

  def isSupportedAggregate(
      aggExpr: AggregateExpression,
      tiDBRelation: TiDBTable,
      blocklist: ExpressionBlocklist): Boolean =
    aggExpr.aggregateFunction match {
      // Average will not push down because Count can't push down
      case _: Average =>
        !aggExpr.isDistinct &&
          aggExpr.aggregateFunction.children
            .forall(isSupportedBasicExpression(_, tiDBRelation, blocklist))
      case _: Sum =>
        !aggExpr.isDistinct &&
          aggExpr.aggregateFunction.children
            .forall(isSupportedBasicExpression(_, tiDBRelation, blocklist))
      case CountSum(_) | PromotedSum(_) | Min(_) | Max(_) =>
        !aggExpr.isDistinct &&
          aggExpr.aggregateFunction.children
            .forall(isSupportedBasicExpression(_, tiDBRelation, blocklist))
      case Count(children) if children.size == 1 =>
        // set can not push down
        if (children.head.isInstanceOf[AttributeReference]) {
          val dataType = tiDBRelation.table.getColumn(children.head.references.head.name).getType
          !dataType.isInstanceOf[SetType] && !aggExpr.isDistinct && children.forall(
            isSupportedBasicExpression(_, tiDBRelation, blocklist))
        } else {
          !aggExpr.isDistinct && children.forall(
            isSupportedBasicExpression(_, tiDBRelation, blocklist))
        }
      case _ => false
    }

  def isSupportedBasicExpression(
      expr: Expression,
      table: TiDBTable,
      blocklist: ExpressionBlocklist): Boolean = {
    if (!BasicExpression.isSupportedExpression(expr, RequestTypes.REQ_TYPE_DAG)) return false

    BasicExpression.convertToTiExpr(expr).fold(false) { expr: TiExpression =>
      MetaResolver.resolve(expr, table.table)
      return SupportedExpressionValidator.isSupportedExpression(expr, blocklist)
    }
  }

  /**
   * Is expression allowed to be pushed down
   *
   * @param expr the expression to examine
   * @return whether expression can be pushed down
   */
  def isPushDownSupported(expr: Expression, source: TiDBTable): Boolean = {
    val nameTypeMap = mutable.HashMap[String, com.pingcap.tikv.types.DataType]()
    source.table.getColumns
      .foreach((info: TiColumnInfo) => nameTypeMap(info.getName) = info.getType)

    if (expr.children.isEmpty) {
      expr match {
        // bit, set and enum type is not allowed to be pushed down
        case attr: AttributeReference if nameTypeMap.contains(attr.name) =>
          return nameTypeMap.get(attr.name).head.isPushDownSupported
        // TODO: Currently we do not support literal null type push down
        // when Constant is ready to support literal null or we have other
        // options, remove this.
        case constant: Literal =>
          return constant.value != null
        case _ => return true
      }
    } else {
      for (expr <- expr.children) {
        if (!isPushDownSupported(expr, source)) {
          return false
        }
      }
    }

    true
  }

  def isSupportedOrderBy(
      expr: Expression,
      source: TiDBTable,
      blocklist: ExpressionBlocklist): Boolean =
    isSupportedBasicExpression(expr, source, blocklist) && isPushDownSupported(expr, source)

  def isSupportedFilter(
      expr: Expression,
      source: TiDBTable,
      blocklist: ExpressionBlocklist): Boolean =
    isSupportedBasicExpression(expr, source, blocklist) && isPushDownSupported(expr, source)

  // if contains UDF / functions that cannot be folded
  def isSupportedGroupingExpr(
      expr: NamedExpression,
      source: TiDBTable,
      blocklist: ExpressionBlocklist): Boolean =
    isSupportedBasicExpression(expr, source, blocklist) && isPushDownSupported(expr, source)
}
