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

package com.pingcap.tikv.expression.visitor;


import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.ComparisonBinaryExpression.NormalizedPredicate;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.key.TypedKey;
import java.util.Objects;
import java.util.Set;


public class IndexRangeBuilder extends DefaultVisitor<RangeSet<TypedKey>, Void> {
  public static Set<Range<TypedKey>> buildRange(Expression predicate) {
    Objects.requireNonNull(predicate, "predicate is null");
    RangeSet<TypedKey> resultRanges = TreeRangeSet.create();
    resultRanges.add(Range.all());
    IndexRangeBuilder visitor = new IndexRangeBuilder();
    Set<Range<TypedKey>> ranges = predicate.accept(visitor, null).asRanges();
    for (Range<TypedKey> range : ranges) {
      resultRanges = resultRanges.subRangeSet(range);
    }
    return resultRanges.asRanges();
  }

  private static void throwOnError(Expression node) {
    final String errorFormat = "Unsupported conversion to Range: %s";
    throw new TiExpressionException(String.format(errorFormat, node));
  }

  protected RangeSet<TypedKey> process(Expression node, Void context) {
    throwOnError(node);
    return null;
  }

  @Override
  protected RangeSet<TypedKey> visit(LogicalBinaryExpression node, Void context) {
    RangeSet<TypedKey> leftRanges = node.getLeft().accept(this, context);
    RangeSet<TypedKey> rightRanges = node.getRight().accept(this, context);
    switch (node.getCompType()) {
      case AND:
        for (Range<TypedKey> range : leftRanges.asRanges()) {
          rightRanges = rightRanges.subRangeSet(range);
        }
        break;
      case OR:
        rightRanges.addAll(leftRanges);
        break;
      case XOR:
        // AND
        RangeSet<TypedKey> intersection = rightRanges;
        for (Range<TypedKey> range : leftRanges.asRanges()) {
          intersection = intersection.subRangeSet(range);
        }
        // full set
        rightRanges.addAll(leftRanges);
        rightRanges.removeAll(intersection);
        break;
      default:
        throwOnError(node);
    }
    return rightRanges;
  }

  @Override
  protected RangeSet<TypedKey> visit(ComparisonBinaryExpression node, Void context) {
    NormalizedPredicate predicate = node.normalize();
    if (predicate == null) {
      throwOnError(node);
    }
    TypedKey literal = predicate.getTypedLiteral();
    RangeSet<TypedKey> ranges = TreeRangeSet.create();

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
    return ranges;
  }
}
