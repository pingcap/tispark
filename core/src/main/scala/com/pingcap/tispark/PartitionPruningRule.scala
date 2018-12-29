package com.pingcap.tispark

import java.util

import com.google.common.collect.{Range, RangeSet, TreeRangeSet}
import com.pingcap.tikv.meta.{TiPartitionDef, TiPartitionInfo, TiTableInfo}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BinaryComparison, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Or, UnaryExpression}

case class AsOrdered[T](val value: T)(implicit ord: Ordering[T]) extends Ordered[AsOrdered[T]] {
  override def compare(that: AsOrdered[T]) = ord.compare(value, that.value)
  def open[T: Ordering](from: T, to: T) = {
    val ord = implicitly[Ordering[T]]
    Range.open(AsOrdered(from), AsOrdered(to))
  }
}

case class PartitionPruningRule(partitionExprs: List[Expression],
                                partitionExpr: Expression,
                                col: String) {
  val ranges = partitionExprs
  val partExpr = partitionExpr
  val columnName = col

  private def buildExprRange(e: Expression): RangeSet[AsOrdered[Long]] =
    e match {
      case GreaterThan(left, right) => {
        val rangeSet = TreeRangeSet.create[AsOrdered[Long]]()
        if (!left.isInstanceOf[Literal] && right.isInstanceOf[Literal]) {
          rangeSet.add(
            Range.open[AsOrdered[Long]](
              AsOrdered(right.eval().asInstanceOf[Number].longValue()),
              AsOrdered(Long.MaxValue)
            )
          )
        }
        if (!right.isInstanceOf[Literal] && left.isInstanceOf[Literal]) {
          rangeSet.add(
            Range.open[AsOrdered[Long]](
              AsOrdered(Long.MinValue),
              AsOrdered(left.eval().asInstanceOf[Number].longValue())
            )
          )
        }
        rangeSet
      }
      case LessThan(left, right) => {
        val rangeSet = TreeRangeSet.create[AsOrdered[Long]]()
        if (!left.isInstanceOf[Literal] && right.isInstanceOf[Literal]) {
          rangeSet.add(
            Range.open[AsOrdered[Long]](
              AsOrdered(Long.MinValue),
              AsOrdered(right.eval().asInstanceOf[Number].longValue())
            )
          )
        }
        if (!right.isInstanceOf[Literal] && left.isInstanceOf[Literal]) {
          rangeSet.add(
            Range.open[AsOrdered[Long]](
              AsOrdered(left.eval().asInstanceOf[Number].longValue()),
              AsOrdered(Long.MaxValue)
            )
          )
        }
        rangeSet
      }
      case Or(left, right) => {
        val leftRangeSet = buildExprRange(left)
        val rightRangeSet = buildExprRange(right)
        leftRangeSet.addAll(rightRangeSet)
        leftRangeSet
      }
      case And(left, right) => {
        val leftRangeSet = buildExprRange(left)
        val rightRangeSet = buildExprRange(right)
        leftRangeSet.removeAll(rightRangeSet.complement())
        leftRangeSet
      }
      case LessThanOrEqual(left, right) => {
        val rangeSet = TreeRangeSet.create[AsOrdered[Long]]()
        var range: Range[AsOrdered[Long]] = null
        if (!left.isInstanceOf[Literal] && right.isInstanceOf[Literal]) {
          rangeSet.add(
            Range.open[AsOrdered[Long]](
              AsOrdered(Long.MinValue),
              AsOrdered(right.eval().asInstanceOf[Number].longValue())
            )
          )
        }
        if (!right.isInstanceOf[Literal] && left.isInstanceOf[Literal]) {
          rangeSet.add(
            Range.open[AsOrdered[Long]](
              AsOrdered(left.eval().asInstanceOf[Number].longValue()),
              AsOrdered(Long.MaxValue)
            )
          )
        }
        rangeSet
      }
      case GreaterThanOrEqual(left, right) => {
        val rangeSet = TreeRangeSet.create[AsOrdered[Long]]()
        if (!left.isInstanceOf[Literal] && right.isInstanceOf[Literal]) {
          rangeSet.add(
            Range.closedOpen[AsOrdered[Long]](
              AsOrdered(right.eval().asInstanceOf[Number].longValue()),
              AsOrdered(Long.MaxValue)
            )
          )
        }
        if (!right.isInstanceOf[Literal] && left.isInstanceOf[Literal]) {
          rangeSet.add(
            Range.openClosed[AsOrdered[Long]](
              AsOrdered(Long.MinValue),
              AsOrdered(left.eval().asInstanceOf[Number].longValue())
            )
          )
        }
        rangeSet
      }
      case EqualTo(left, right) => {
        val rangeSet = TreeRangeSet.create[AsOrdered[Long]]()
        if (!left.isInstanceOf[Literal] && right.isInstanceOf[Literal]) {
          rangeSet.add(
            Range.closed[AsOrdered[Long]](
              AsOrdered(right.eval(null).asInstanceOf[Number].longValue()),
              AsOrdered(right.eval(null).asInstanceOf[Number].longValue())
            )
          )
        }
        if (!right.isInstanceOf[Literal] && left.isInstanceOf[Literal]) {
          rangeSet.add(
            Range.closed[AsOrdered[Long]](
              AsOrdered(left.eval(null).asInstanceOf[Number].longValue()),
              AsOrdered(left.eval(null).asInstanceOf[Number].longValue())
            )
          )
        }
        rangeSet
      }
    }

  // exprs should be in the form of partition def + all filters got
  // involved with partition column.
  def canBePruned(exprs: Seq[Expression]): Boolean = {
    val rangePointsSet = TreeRangeSet.create[AsOrdered[Long]]()
    exprs.foreach((e) => {
      if (rangePointsSet.isEmpty) {
        rangePointsSet.addAll(buildExprRange(e))
      } else {
        val exprRangeSet = buildExprRange(e)
        rangePointsSet.removeAll(exprRangeSet.complement())
      }
    })
    rangePointsSet.isEmpty
  }

  // expression parsed by parseExpression will leave attribute as unresolved.
  // We take advantage of this info and replace all [[UnresolvedAttribute]] as literal.
  // Later, we can call eval on this expression.
  // +(year(purchased), 1)
  private def replaceUnresolvedAttrInPartExprWithLiteral(expr: Expression): Expression =
    partExpr transformUp {
      case l @ Literal(_, _)         => { l }
      case UnresolvedAttribute(name) => { expr.asInstanceOf[Literal] }
    }

  // it only rewrite the literal part and leave attribute unchanged.
  // For date < '1992-01-01', expected result is 'date < 1992)' if part_expr is year.
  private def evalLiteralInBinaryComparison(expr: Expression): Expression =
    expr match {
      case o @ Or(left, right) => {
        o.withNewChildren(
          Seq(evalLiteralInBinaryComparison(left), evalLiteralInBinaryComparison(right))
        )
      }
      case b @ BinaryComparison(left, right) => {
        var newB: Expression = null
        // only allowing `attribute > literal` or 'literal > attribute' form entering if branch.
        if (left.isInstanceOf[AttributeReference] && right.isInstanceOf[Literal]) {
          val evaledVal = replaceUnresolvedAttrInPartExprWithLiteral(right).eval()
          newB = b.withNewChildren(
            Seq(left, Literal.create(evaledVal))
          )
        }
        if (!right.isInstanceOf[Literal] && left.isInstanceOf[AttributeReference]) {
          newB = b.withNewChildren(
            Seq(Literal.create(replaceUnresolvedAttrInPartExprWithLiteral(left)), right)
          )
        }
        newB
      }
    }

  // partition pruning only allows simple expression such as date < '1992-01-01'.
  // `to_seconds(date)` < '1992-01-01' will not be pruned.
  private def validExprCheck(exprs: Seq[Expression]): Boolean =
    return exprs.find((e) => e.isInstanceOf[UnaryExpression]).isEmpty

  def pruning(accessConds: Seq[Expression], table: TiTableInfo): TiPartitionInfo = {
    val filteredAccessConds =
      accessConds.filter((e) => !e.isInstanceOf[IsNull] && !e.isInstanceOf[IsNotNull])
    // this step applies partition expression on where condition.
    // If we have a partition expr: year(date) - 1, when it comes to date < '1992-10-10',
    // we need apply `year(date) - 1` on '1992-10-10'.
    val transformedAccessConds = filteredAccessConds.map(evalLiteralInBinaryComparison)

    val residualPartDefs: java.util.List[TiPartitionDef] = new util.ArrayList[TiPartitionDef]()
    ranges.zipWithIndex.foreach {
      case (e, i) =>
        if (!canBePruned(transformedAccessConds :+ e)) {
          residualPartDefs.add(table.getPartitionInfo.getDefs.get(i))
        }
    }
    val prunedPartInfo: TiPartitionInfo =
      table.getPartitionInfo().clone
    prunedPartInfo.setDefs(residualPartDefs)
    prunedPartInfo
  }
}
