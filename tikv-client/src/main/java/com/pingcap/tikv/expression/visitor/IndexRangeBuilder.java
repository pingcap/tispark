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
import com.pingcap.tikv.expression.*;
import com.pingcap.tikv.expression.ComparisonBinaryExpression.NormalizedPredicate;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


public class IndexRangeBuilder extends DefaultVisitor<RangeSet<TypedKey>, Void> {

  private final Map<ColumnRef, Integer> lengths; // length of corresponding ColumnRef

  public IndexRangeBuilder(TiTableInfo table, TiIndexInfo index) {
    Map<ColumnRef, Integer> result = new HashMap<>();
    if (table != null && index != null) {
      for (TiIndexColumn indexColumn : index.getIndexColumns()) {
        ColumnRef columnRef = ColumnRef.create(indexColumn.getName(), table);
        result.put(columnRef, (int) indexColumn.getLength());
      }
    }
    this.lengths = result;
  }

  public Set<Range<TypedKey>> buildRange(Expression predicate) {
    Objects.requireNonNull(predicate, "predicate is null");
    return predicate.accept(this, null).asRanges();
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
    // In order to match a prefix index, we have to cut the literal by prefix length.
    // e.g., for table t:
    // CREATE TABLE `t` {
    //     `b` VARCHAR(10) DEFAULT NULL,
    //     KEY `prefix_index` (`b`(2))
    // }
    //
    // b(2) > "bbc" -> ["bb", +∞)
    // b(2) >= "bbc" -> ["bb", +∞)
    // b(2) < "bbc" -> (-∞, "bb"]
    // b(2) <= "bbc" -> (-∞, "bb"]
    // b(2) = "bbc" -> ["bb", "bb"]
    // b(2) > "b" -> ["b", +∞)
    // b(2) >= "b" -> ["b", +∞)
    // b(2) < "b" -> (-∞, "b"]
    // b(2) <= "b" -> (-∞, "b"]
    //
    // For varchar, `b`(2) will take first two characters(bytes) as prefix index.
    // TODO: Note that TiDB only supports UTF-8, we need to check if prefix index behave differently under other encoding methods
    int prefixLen = lengths.getOrDefault(predicate.getColumnRef(), DataType.UNSPECIFIED_LEN);
    TypedKey literal = predicate.getTypedLiteral(prefixLen);
    RangeSet<TypedKey> ranges = TreeRangeSet.create();

    if (prefixLen != DataType.UNSPECIFIED_LEN) {
      // With prefix length specified, the filter is loosen and so should the ranges
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
          // Simply break here because an empty RangeSet will be evaluated as a full range
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
  protected RangeSet<TypedKey> visit(StringRegExpression node, Void context) {
    ColumnRef columnRef = node.getColumnRef();
    // In order to match a prefix index, we have to cut the literal by prefix length.
    // e.g., for table t:
    // CREATE TABLE `t` {
    //     `c1` VARCHAR(10) DEFAULT NULL,
    //     KEY `prefix_index` (`c`(2))
    // }
    // when the predicate is `c1` LIKE 'abc%', the index range should be ['ab', 'ab'].
    // when the predicate is `c1` LIKE 'a%', the index range should be ['a', 'b').
    // for varchar, `c1`(2) will take first two characters(bytes) as prefix index.
    // TODO: Note that TiDB only supports UTF-8, we need to check if prefix index behave differently under other encoding methods
    int prefixLen = lengths.getOrDefault(columnRef, DataType.UNSPECIFIED_LEN);
    TypedKey literal = node.getTypedLiteral(prefixLen);
    RangeSet<TypedKey> ranges = TreeRangeSet.create();

    switch (node.getRegType()) {
      case STARTS_WITH:
        ranges.add(Range.atLeast(literal).intersection(Range.lessThan(literal.next(prefixLen))));
        break;
      default:
        throwOnError(node);
    }
    return ranges;
  }
}
