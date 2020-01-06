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

import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.ComparisonBinaryExpression.NormalizedPredicate;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.key.TypedKey;
import java.util.Set;

/**
 * Apply partition pruning rule on filter condition. Partition pruning is based on a simple idea and
 * can be described as "Do not scan partitions where there can be no matching values". Currently
 * only range partition pruning is supported(range column on multiple columns is not supported at
 * TiDB side, so we can't optimize this yet).
 */
@SuppressWarnings("UnstableApiUsage")
public class PrunedPartitionBuilder extends RangeSetBuilder<TypedKey> {
  private final Set<ColumnRef> partExprColRefs;

  public PrunedPartitionBuilder(Set<ColumnRef> partExprColRefs) {
    this.partExprColRefs = partExprColRefs;
  }

  protected RangeSet<TypedKey> process(Expression node, Void context) {
    throwOnError(node);
    return null;
  }

  @Override
  // This deals with partition definition's 'lessthan' is "maxvalue".
  protected RangeSet<TypedKey> visit(Constant node, Void context) {
    RangeSet<TypedKey> ranges = TreeRangeSet.create();
    if (node.getValue() instanceof Number) {
      long val = ((Number) node.getValue()).longValue();
      if (val == 1) {
        return ranges.complement();
      }
      return ranges;
    }
    return ranges;
  }

  @Override
  protected RangeSet<TypedKey> visit(ComparisonBinaryExpression node, Void context) {
    NormalizedPredicate predicate = node.normalize();
    // when meet a comparison binary expression cannot be normalized
    // which indicates it cannot be pruned such as a > b + 1
    if (predicate == null) return TreeRangeSet.<TypedKey>create().complement();
    if (!partExprColRefs.contains(predicate.getColumnRef()))
      return TreeRangeSet.<TypedKey>create().complement();
    TypedKey literal;
    literal = predicate.getTypedLiteral(-1);
    return visitComparisonBinaryExpr(node, context, literal, false);
  }
}
