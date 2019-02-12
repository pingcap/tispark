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
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.expression.visitor.CanBePrunedValidator.PartPruningContext;
import com.pingcap.tikv.meta.TiPartitionDef;
import com.pingcap.tikv.meta.TiPartitionInfo;
import com.pingcap.tikv.meta.TiPartitionInfo.PartitionType;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.parser.TiParser;
import com.pingcap.tikv.predicates.PredicateUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PrunedPartitionBuilder extends RangeBuilder<Long> {

  private static Expression partExpr;
  private static List<Expression> partExprs;

  protected RangeSet<Long> process(Expression node, Void context) {
    throwOnError(node);
    return null;
  }

  /**
   * return false if table cannot be pruning or partition table is not enabled. Return true if
   * partition pruning can be applied.
   *
   * @param tblInfo is table info which is used for creating columnRef.
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

    partExprs = generateRangePartExprs(tblInfo);
    // Ideally, part expr should be parsed by a parser
    // but it requires a lot work, we chose a brute force way
    // to workaround.
    boolean canBePruned;
    if (partExpr instanceof ColumnRef) {
      // skip not(isnull(col)) case
      canBePruned = CanBePrunedValidator.canBePruned(filter, new PartPruningContext(partExpr));
    } else {
      canBePruned = false;
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
  protected RangeSet<Long> visit(ComparisonBinaryExpression node, Void context) {
    NormalizedPredicate predicate = node.normalize();
    if (!predicate.getColumnRef().equals(partExpr)) return TreeRangeSet.<Long>create().complement();
    Long literal;
    if (predicate.getValue().getValue() instanceof Number) {
      literal = ((Number) predicate.getValue().getValue()).longValue();
      return comparisionBinaryExprVisit(node, context, literal, false);
    }
    return TreeRangeSet.create();
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

  private List<TiPartitionDef> pruneRangeNormalPart(TiTableInfo tableInfo, Expression cnfExpr) {
    TiPartitionInfo partInfo = tableInfo.getPartitionInfo();
    Objects.requireNonNull(cnfExpr, "cnf expression cannot be null at pruning stage");
    RangeSet<Long> filterRange = buildRange(cnfExpr);
    List<TiPartitionDef> pDefs = new ArrayList<>();
    for (int i = 0; i < partExprs.size(); i++) {
      Expression partExpr = partExprs.get(i);
      RangeSet<Long> partRange = buildRange(partExpr);
      partRange.removeAll(filterRange.complement());
      if (!partRange.isEmpty()) {
        // part range is empty indicates this partition can be pruned.
        pDefs.add(partInfo.getDefs().get(i));
      }
    }
    return pDefs;
  }

  /**
   * When table is a partition table and its type is range. We use this method to do the pruning.
   * Range partition has two types: 1. range 2. range column. If it is the first case,
   * pruneRangeNormalPart will be called. Otherwise pruneRangeColPart will be called. For now, we
   * simply skip range column partition case. TODO: support partition pruning on range column later.
   *
   * @param tableInfo is used when we resolve column. See {@code ColumnRef}
   * @param filters is where condition belong to a select statement.
   * @return a pruned partition for scanning.
   */
  // pruneRangePart will prune range partition that where conditions never access.
  private List<TiPartitionDef> pruneRangePart(TiTableInfo tableInfo, List<Expression> filters) {
    filters = extractLogicalOrComparisonExpr(filters);
    Expression cnfExpr = PredicateUtils.mergeCNFExpressions(filters);
    if (!canBePruned(tableInfo, cnfExpr)) {
      return tableInfo.getPartitionInfo().getDefs();
    }

    List<String> columnInfos = tableInfo.getPartitionInfo().getColumns();
    boolean isRangeCol = columnInfos != null & columnInfos.size() > 0;
    if (isRangeCol) {
      // range column partition pruning will be support later.
      return tableInfo.getPartitionInfo().getDefs();
    }

    return pruneRangeNormalPart(tableInfo, cnfExpr);
  }

  private static List<Expression> generateRangePartExprs(TiTableInfo tableInfo) {
    TiPartitionInfo partInfo = tableInfo.getPartitionInfo();
    List<Expression> partExprs = new ArrayList<>();
    TiParser parser = new TiParser(tableInfo);
    String partExprStr = tableInfo.getPartitionInfo().getExpr();
    partExpr = parser.parseExpression(partExprStr);
    // when it is not range column case, only first element stores useful info.
    for (int i = 0; i < partInfo.getDefs().size(); i++) {
      TiPartitionDef pDef = partInfo.getDefs().get(i);
      if (i == 0) {
        String literal = pDef.getLessThan().get(0);
        partExprs.add(parser.parseExpression(partExprStr + "<" + literal));
      } else {
        String previous = partInfo.getDefs().get(i - 1).getLessThan().get(0);
        String current = pDef.getLessThan().get(0);
        String and =
            String.format("%s and %s", partExprStr + ">=" + previous, partExprStr + "<" + current);
        partExprs.add(parser.parseExpression(and));
      }
    }
    return partExprs;
  }
}
