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
import com.google.common.primitives.Longs;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.ComparisonBinaryExpression.NormalizedPredicate;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.IsNull;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.expression.Not;
import com.pingcap.tikv.expression.visitor.CanBePrunedValidator.ContextPartExpr;
import com.pingcap.tikv.meta.TiPartitionDef;
import com.pingcap.tikv.meta.TiPartitionInfo;
import com.pingcap.tikv.meta.TiPartitionInfo.PartitionType;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.predicates.PredicateUtils;
import java.util.ArrayList;
import java.util.List;

public class PrunedPartitionBuilder extends DefaultVisitor<RangeSet<Long>, Void> {

  private static Expression partExpr;

  private static void throwOnError(Expression node) {
    final String errorFormat = "Unsupported conversion to Range: %s";
    throw new TiExpressionException(String.format(errorFormat, node));
  }

  protected RangeSet<Long> process(Expression node, Void context) {
    throwOnError(node);
    return null;
  }

  /**
   * return false if table cannot be pruning or partition table is not enabled. return true if
   * partition expression is simple.
   *
   * @param tblInfo is table info which is used for creaeting columnRef.
   * @param filter is a where condition. It must be a cnf and does not contain not or isnull.
   * @return true if partition pruning can apply on filter.
   */
  private static boolean canBePruned(TiTableInfo tblInfo, Expression filter) {
    if (!tblInfo.isPartitionEnabled()) {
      return false;
    }

    if (TiPartitionInfo.toPartType((int) tblInfo.getPartitionInfo().getType())
        != PartitionType.RangePartition) {
      return false;
    }

    // if query is select * from t, then filter will be null.
    if (filter == null) return false;

    // Ideally, part expr should be parsed by a parser
    // but it requires a lot work, we chose a brute force way
    // to workaround.
    boolean canBePruned = true;
    partExpr = ColumnRef.create(tblInfo.getPartitionInfo().getExpr(), tblInfo);
    if (!tblInfo.getPartitionInfo().getExpr().contains("(")) {
      // skip not(isnull(col)) case
      canBePruned = CanBePrunedValidator.canBePruned(filter, new ContextPartExpr(partExpr));
    }

    return canBePruned;
  }

  private static List<Expression> extractLogicalOrComparisonExpr(List<Expression> filters) {
    List<Expression> filteredFilters = new ArrayList<>();
    for (Expression expr : filters) {
      if (expr instanceof LogicalBinaryExpression || expr instanceof ComparisonBinaryExpression) {
        filteredFilters.add(expr);
      }
    }
    return filteredFilters;
  }

  @Override
  @SuppressWarnings("Duplicates")
  protected RangeSet<Long> visit(ComparisonBinaryExpression node, Void context) {
    NormalizedPredicate predicate = node.normalize();
    RangeSet<Long> ranges = TreeRangeSet.create();
    if (!predicate.getColumnRef().equals(partExpr)) return TreeRangeSet.<Long>create().complement();
    Long literal = (Long) predicate.getValue().getValue();
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

  @Override
  protected RangeSet<Long> visit(LogicalBinaryExpression node, Void context) {
    RangeSet<Long> leftRanges = node.getLeft().accept(this, context);
    RangeSet<Long> rightRanges = node.getRight().accept(this, context);
    switch (node.getCompType()) {
      case AND:
        rightRanges.removeAll(leftRanges.complement());
        break;
      case OR:
        rightRanges.addAll(leftRanges);
        break;
      case XOR:
        // AND
        RangeSet<Long> intersection = TreeRangeSet.create(rightRanges);
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

  public List<TiPartitionDef> prune(TiTableInfo tableInfo, List<Expression> filters) {
    switch (TiPartitionInfo.toPartType((int) tableInfo.getPartitionInfo().getType())) {
      case RangePartition:
        return pruneRangePart(tableInfo, filters);
      case ListPartition:
        return pruneListPart(tableInfo, filters);
      case HashPartition:
        return pruneHashPart(tableInfo, filters);
    }

    throw new UnsupportedOperationException("cannot prune under invalid partition table");
  }

  // pruneListPart will prune list partition that where conditions never access.
  private List<TiPartitionDef> pruneListPart(TiTableInfo tableInfo, List<Expression> filters) {
    return tableInfo.getPartitionInfo().getDefs();
  }

  // pruneHashPart will prune hash partition that where conditions never access.
  private List<TiPartitionDef> pruneHashPart(TiTableInfo tableInfo, List<Expression> filters) {
    return tableInfo.getPartitionInfo().getDefs();
  }

  // pruneRangePart will prune range partition that where conditions never access.
  private List<TiPartitionDef> pruneRangePart(TiTableInfo tableInfo, List<Expression> filters) {
    filters = extractLogicalOrComparisonExpr(filters);

    Expression cnfExpr = PredicateUtils.mergeCNFExpressions(filters);
    if (!canBePruned(tableInfo, cnfExpr)) {
      return tableInfo.getPartitionInfo().getDefs();
    }

    List<Expression> partExprs = generatePartExprs(tableInfo);
    TiPartitionInfo partInfo = tableInfo.getPartitionInfo();
    RangeSet<Long> filterRange = buildPartRange(cnfExpr);
    List<TiPartitionDef> pDefs = new ArrayList<>();
    for (int i = 0; i < partExprs.size(); i++) {
      Expression partExpr = partExprs.get(i);
      RangeSet<Long> partRange = buildPartRange(partExpr);
      partRange.removeAll(filterRange.complement());
      if (!partRange.isEmpty()) {
        // part range is empty indicates this partition can be pruned.
        pDefs.add(partInfo.getDefs().get(i));
      }
    }
    return pDefs;
  }

  private RangeSet<Long> buildPartRange(Expression filter) {
    return filter.accept(this, null);
  }

  private static List<Expression> generatePartExprs(TiTableInfo tableInfo) {
    // only support column ref for now. Fn Expr will be supported later.
    TiPartitionInfo partInfo = tableInfo.getPartitionInfo();
    List<Expression> partExprs = new ArrayList<>();
    for (int i = 0; i < partInfo.getDefs().size(); i++) {
      TiPartitionDef pDef = partInfo.getDefs().get(i);
      Constant cst = Constant.create(Longs.tryParse(pDef.getLessThan().get(0)));
      if (i == 0) {
        partExprs.add(ComparisonBinaryExpression.lessThan(partExpr, cst));
      } else {
        Constant previous =
            Constant.create(Longs.tryParse(partInfo.getDefs().get(i - 1).getLessThan().get(0)));
        Constant current =
            Constant.create(Longs.tryParse(partInfo.getDefs().get(i).getLessThan().get(0)));
        partExprs.add(
            LogicalBinaryExpression.and(
                ComparisonBinaryExpression.greaterEqual(partExpr, previous),
                ComparisonBinaryExpression.lessThan(partExpr, current)));
      }
    }
    return partExprs;
  }
}
