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

import static java.util.Objects.requireNonNull;

import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.ComparisonBinaryExpression.NormalizedPredicate;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.expression.StringRegExpression;
import com.pingcap.tikv.meta.TiIndexColumn;

/**
 * Test if a predicate matches and index column entirely and can be convert to index related ranges
 * If a predicate matches only partially, it returns false
 */
public class IndexMatcher extends DefaultVisitor<Boolean, Void> {
  private final boolean matchEqualTestOnly;
  private final TiIndexColumn indexColumn;

  private IndexMatcher(TiIndexColumn indexColumn, boolean matchEqualTestOnly) {
    this.matchEqualTestOnly = matchEqualTestOnly;
    this.indexColumn = requireNonNull(indexColumn, "index column is null");
  }

  public static IndexMatcher equalOnlyMatcher(TiIndexColumn indexColumn) {
    return new IndexMatcher(indexColumn, true);
  }

  public static IndexMatcher matcher(TiIndexColumn indexColumn) {
    return new IndexMatcher(indexColumn, false);
  }

  public boolean match(Expression expression) {
    return expression.accept(this, null);
  }

  @Override
  protected Boolean process(Expression node, Void context) {
    return false;
  }

  @Override
  protected Boolean visit(ColumnRef node, Void context) {
    String indexColumnName = indexColumn.getName();
    return node.matchName(indexColumnName);
  }

  @Override
  protected Boolean visit(ComparisonBinaryExpression node, Void context) {
    switch (node.getComparisonType()) {
      case LESS_THAN:
      case LESS_EQUAL:
      case GREATER_THAN:
      case GREATER_EQUAL:
      case NOT_EQUAL:
        if (matchEqualTestOnly) {
          return false;
        }
      case EQUAL:
        NormalizedPredicate predicate = node.normalize();
        if (predicate == null) {
          return false;
        }
        return predicate.getColumnRef().accept(this, context);
      default:
        return false;
    }
  }

  @Override
  protected Boolean visit(StringRegExpression node, Void context) {
    switch (node.getRegType()) {
        // If the predicate is StartsWith(col, 'a'), this predicate
        // indicates a range of ['a', +∞) which can be used by index scan
      case STARTS_WITH:
        if (matchEqualTestOnly) {
          return false;
        }
        return node.getLeft().accept(this, context);
      default:
        return false;
    }
  }

  @Override
  protected Boolean visit(LogicalBinaryExpression node, Void context) {
    switch (node.getCompType()) {
      case AND:
        if (matchEqualTestOnly) {
          return false;
        }
      case OR:
      case XOR:
        return node.getLeft().accept(this, context) && node.getRight().accept(this, context);
      default:
        return false;
    }
  }
}
