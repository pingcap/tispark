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

package com.pingcap.tikv.expression.visitor;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.ComparisonBinaryExpression.NormalizedPredicate;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import java.util.Objects;

/**
 * A builder can build a range set of type {@code C}. It also extends {@code DefaultVisitor} and
 * override and {@code LogicalBinaryExpression}'s visit. For {@code ComparisonBinaryExpression}, we
 * cannot just override it because {@code IndexRangeSetBuilder} and {@code LogicalBinaryExpression}
 * has different behavior. A method {@code visitComparisonBinaryExpr} is added with extra boolean
 * variable to control the behavior.
 */
@SuppressWarnings("UnstableApiUsage")
public class RangeSetBuilder<C extends Comparable> extends DefaultVisitor<RangeSet<C>, Void> {

  static void throwOnError(Expression node) {
    final String errorFormat = "Unsupported conversion to Range: %s";
    throw new TiExpressionException(String.format(errorFormat, node));
  }

  /**
   * visits {@code ComparisonBinaryExpression} expression and constructs a range set.
   *
   * @param node represents a {@code ComparisonBinaryExpression}.
   * @param context represents a context during visiting process. It is not being used in this
   *     method.
   * @param literal represents a comparable value.
   * @param loose If prefix length is specified, then filter is loose, so is the range.
   * @return a range set.
   */
  RangeSet<C> visitComparisonBinaryExpr(
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
        // We need make a copy of rightRanges rather than assign the pointer
        // to intersection since we need modify intersection later.
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

  public RangeSet<C> buildRange(Expression predicate) {
    Objects.requireNonNull(predicate, "predicate is null");
    return predicate.accept(this, null);
  }

  protected RangeSet<C> process(Expression node, Void context) {
    throwOnError(node);
    return null;
  }
}
