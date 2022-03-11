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

package com.pingcap.tikv.predicates;

import static com.pingcap.tikv.expression.LogicalBinaryExpression.and;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.Visitor;
import com.pingcap.tikv.expression.visitor.DefaultVisitor;
import com.pingcap.tikv.expression.visitor.IndexRangeSetBuilder;
import com.pingcap.tikv.key.CompoundKey;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class PredicateUtils {
  public static Expression mergeCNFExpressions(List<Expression> exprs) {
    requireNonNull(exprs, "Expression list is null");
    if (exprs.size() == 0) return null;
    if (exprs.size() == 1) return exprs.get(0);

    return and(exprs.get(0), mergeCNFExpressions(exprs.subList(1, exprs.size())));
  }

  public static Set<ColumnRef> extractColumnRefFromExpression(Expression expr) {
    Set<ColumnRef> columnRefs = new HashSet<>();
    Visitor<Void, Set<ColumnRef>> visitor =
        new DefaultVisitor<Void, Set<ColumnRef>>() {
          @Override
          protected Void visit(ColumnRef node, Set<ColumnRef> context) {
            context.add(node);
            return null;
          }
        };

    expr.accept(visitor, columnRefs);
    return columnRefs;
  }

  /**
   * Build index ranges from access points and access conditions
   *
   * @param pointPredicates conditions converting to a single point access
   * @param rangePredicate conditions converting to a range
   * @return Index Range for scan
   */
  public static List<IndexRange> expressionToIndexRanges(
      List<Expression> pointPredicates,
      Optional<Expression> rangePredicate,
      TiTableInfo table,
      TiIndexInfo index) {
    requireNonNull(pointPredicates, "pointPredicates is null");
    requireNonNull(rangePredicate, "rangePredicate is null");
    ImmutableList.Builder<IndexRange> builder = ImmutableList.builder();
    IndexRangeSetBuilder indexRangeBuilder = new IndexRangeSetBuilder(table, index);

    if (pointPredicates.size() != 0) {
      List<Key> pointKeys = expressionToPoints(pointPredicates, table, index);
      for (Key key : pointKeys) {
        if (rangePredicate.isPresent()) {
          Set<Range<TypedKey>> ranges =
              indexRangeBuilder.buildRange(rangePredicate.get()).asRanges();
          for (Range<TypedKey> range : ranges) {
            builder.add(new IndexRange(key, range));
          }
        } else {
          // no predicates with point keys leads to empty range encoding
          builder.add(new IndexRange(key, null));
        }
      }
    } else {
      if (rangePredicate.isPresent()) {
        Set<Range<TypedKey>> ranges = indexRangeBuilder.buildRange(rangePredicate.get()).asRanges();
        for (Range<TypedKey> range : ranges) {
          builder.add(new IndexRange(null, range));
        }
      } else {
        // no filter at all means full range
        builder.add(new IndexRange(null, Range.all()));
      }
    }

    return builder.build();
  }

  /**
   * Turn access conditions into list of points Each condition is bound to single key We pick up
   * single condition for each index key and disregard if multiple EQ conditions in DNF
   *
   * @param pointPredicates expressions that convertible to access points
   * @return access points for each index
   */
  private static List<Key> expressionToPoints(
      List<Expression> pointPredicates, TiTableInfo table, TiIndexInfo index) {
    requireNonNull(pointPredicates, "pointPredicates cannot be null");

    List<Key> resultKeys = new ArrayList<>();
    IndexRangeSetBuilder indexRangeBuilder = new IndexRangeSetBuilder(table, index);

    for (int i = 0; i < pointPredicates.size(); i++) {
      Expression predicate = pointPredicates.get(i);
      try {
        // each expr will be expand to one or more points
        Set<Range<TypedKey>> ranges = indexRangeBuilder.buildRange(predicate).asRanges();
        List<Key> points = rangesToPoint(ranges);
        resultKeys = joinKeys(resultKeys, points);
      } catch (Exception e) {
        throw new TiExpressionException(
            String.format("Error converting access points %s", predicate), e);
      }
    }
    return resultKeys;
  }

  // Convert ranges of equal condition points to List of TypedKeys
  private static List<Key> rangesToPoint(Set<Range<TypedKey>> ranges) {
    requireNonNull(ranges, "ranges is null");
    ImmutableList.Builder<Key> builder = ImmutableList.builder();
    for (Range<TypedKey> range : ranges) {
      // test if range is a point
      if (range.hasLowerBound()
          && range.hasUpperBound()
          && range.lowerEndpoint().equals(range.upperEndpoint())) {
        builder.add(range.lowerEndpoint());
      } else {
        throw new TiExpressionException("Cannot convert range to point");
      }
    }
    return builder.build();
  }

  private static List<Key> joinKeys(List<Key> lhsKeys, List<Key> rhsKeys) {
    requireNonNull(lhsKeys, "lhsKeys is null");
    requireNonNull(rhsKeys, "rhsKeys is null");
    if (lhsKeys.isEmpty()) {
      return rhsKeys;
    }
    if (rhsKeys.isEmpty()) {
      return lhsKeys;
    }
    ImmutableList.Builder<Key> builder = ImmutableList.builder();
    for (Key lKey : lhsKeys) {
      for (Key rKey : rhsKeys) {
        builder.add(CompoundKey.concat(lKey, rKey));
      }
    }
    return builder.build();
  }
}
