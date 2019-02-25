package com.pingcap.tikv.exception;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.ComparisonBinaryExpression.NormalizedPredicate;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.expression.visitor.DefaultVisitor;

public class RangeBuilder<C extends Comparable> extends DefaultVisitor<RangeSet<C>, Void> {

  protected RangeSet<C> comparisionBinaryExprVisit(
      ComparisonBinaryExpression node, Void context, C literal, boolean loose) {
    NormalizedPredicate predicate = node.normalize();
    RangeSet<C> ranges = TreeRangeSet.create();
    if (loose) {
      switch (predicate.getType()) {
        case GREATER_THAN:
        case GREATER_EQUAL:
          ranges.add(Range.atLeast(literal));
          break;
        case LESS_THAN:
        case LESS_EQUAL:
          ranges.add(Range.atMost(literal));
          break;
        case EQUAL:
          ranges.add(Range.singleton(literal));
          break;
        case NOT_EQUAL:
          // Should return full range because prefix index predicate for NOT_EQUAL
          // will be split into an NOT_EQUAL filter and a full range scan
          ranges.add(Range.all());
          break;
        default:
          throwOnError(node);
      }
    } else {
      switch (predicate.getType()) {
        case GREATER_THAN:
          ranges.add(Range.greaterThan(literal));
          break;
        case GREATER_EQUAL:
          ranges.add(Range.atLeast(literal));
          break;
        case LESS_THAN:
          ranges.add(Range.lessThan(literal));
          break;
        case LESS_EQUAL:
          ranges.add(Range.atMost(literal));
          break;
        case EQUAL:
          ranges.add(Range.singleton(literal));
          break;
        case NOT_EQUAL:
          ranges.add(Range.lessThan(literal));
          ranges.add(Range.greaterThan(literal));
          break;
        default:
          throwOnError(node);
      }
    }

    return ranges;
  }

  @Override
  protected RangeSet<C> visit(LogicalBinaryExpression node, Void context) {
    RangeSet<C> leftRanges = node.getLeft().accept(this, context);
    RangeSet<C> rightRanges = node.getRight().accept(this, context);
    switch (node.getCompType()) {
      case AND:
        rightRanges.removeAll(leftRanges.complement());
        break;
      case OR:
        rightRanges.addAll(leftRanges);
        break;
      case XOR:
        // AND
        RangeSet<C> intersection = TreeRangeSet.create(rightRanges);
        intersection.removeAll(leftRanges.complement());
        // full set
        rightRanges.addAll(leftRanges);
        rightRanges.removeAll(intersection);
        break;
      default:
        throwOnError(node);
    }
    return rightRanges;
  }

  private static void throwOnError(Expression node) {
    final String errorFormat = "Unsupported conversion to Range: %s";
    throw new TiExpressionException(String.format(errorFormat, node));
  }

  protected RangeSet<C> process(Expression node, Void context) {
    throwOnError(node);
    return null;
  }
}
