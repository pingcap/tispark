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
import com.pingcap.tikv.meta.TiPartitionDef;
import com.pingcap.tikv.meta.TiPartitionInfo;
import com.pingcap.tikv.meta.TiPartitionInfo.PartitionType;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.parser.TiParser;
import com.pingcap.tikv.predicates.PredicateUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Apply partition pruning rule on filter condition. Partition pruning is based on a simple idea and
 * can be described as "Do not scan partitions where there can be no matching values". Currently
 * only range partition pruning is supported(range column on mutiple columns is not supported at
 * TiDB side, so we can't optimize this yet).
 */
public class PrunedPartitionBuilder extends RangeSetBuilder<Long> {

  private static Expression partExpr;
  private static Set<ColumnRef> partExprColRefs;
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

    // generate partition expressions
    partExprs = generateRangePartExprs(tblInfo);

    return true;
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
    if (!partExprColRefs.contains(predicate.getColumnRef()))
      return TreeRangeSet.<Long>create().complement();
    Long literal;
    if (predicate.getValue().getValue() instanceof Number) {
      literal = ((Number) predicate.getValue().getValue()).longValue();
      return comparisonBinaryExprVisit(node, context, literal, false);
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

    // we need rewrite filter expression. This step is designed to  deal with
    // y < '1885-10-10' where y is a date type. Rewrite will apply partition expression
    // on the constant part. After rewriting, it becomes y < 1995;
    PartAndFilterExprRewriter expressionRewriter = new PartAndFilterExprRewriter(partExpr);
    cnfExpr = expressionRewriter.rewrite(cnfExpr);
    // if we find an unsupported partition function, we downgrade to scan all partitions.
    if (expressionRewriter.isUnsupportedPartFnFound()) {
      return partInfo.getDefs();
    }

    RangeSet<Long> filterRange = buildRange(cnfExpr);

    List<TiPartitionDef> pDefs = new ArrayList<>();
    for (int i = 0; i < partExprs.size(); i++) {
      Expression partExpr = partExprs.get(i);
      // when we build range, we still need rewrite partition expression.
      // If we have a year(purchased) < 1995 which cannot be normalized, we need
      // to rewrite it into purchased < 1995 to let RangeSetBuilder be happy.
      RangeSet<Long> partRange = buildRange(expressionRewriter.rewrite(partExpr));
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
      // TODO: range column partition pruning will be support later.
      // pruning can also be performed for WHERE conditions that involve comparisons of the
      // preceding types on multiple columns for tables that use RANGE COLUMNS or LIST COLUMNS
      // partitioning.
      // This type of optimization can be applied whenever the partitioning expression consists
      // of an equality or a range which can be reduced to a set of equalities, or when
      // the partitioning expression represents an increasing or decreasing relationship.
      // Pruning can also be applied for tables partitioned on a DATE or DATETIME column
      // when the partitioning expression uses the YEAR() or TO_DAYS() function.
      // Pruning can also be applied for such tables when the partitioning expression uses
      // the TO_SECONDS() function
      return tableInfo.getPartitionInfo().getDefs();
    }

    return pruneRangeNormalPart(tableInfo, cnfExpr);
  }

  // say we have a partitioned table with the following partition definitions with year(y) as
  // partition expression:
  // 1. p0 less than 1995
  // 2. p1 less than 1996
  // 3. p2 less than maxvalue
  // Above infos, after this function, will become the following:
  // 1. p0: year(y) < 1995
  // 2. p1: 1995 <= year(y) and year(y) < 1996
  // 3. p2: 1996 <= year(y) and true
  // true will become {@Code Constant} 1.
  private static List<Expression> generateRangePartExprs(TiTableInfo tableInfo) {
    TiPartitionInfo partInfo = tableInfo.getPartitionInfo();
    List<Expression> partExprs = new ArrayList<>();
    TiParser parser = new TiParser(tableInfo);
    // check year expression
    // rewrite filter condition
    // purchased > '1995-10-10'
    // year(purchased) > year('1995-10-10')
    // purchased > 1995
    String partExprStr = tableInfo.getPartitionInfo().getExpr();
    partExpr = parser.parseExpression(partExprStr);
    partExprColRefs = PredicateUtils.extractColumnRefFromExpression(partExpr);
    // when it is not range column case, only first element stores useful info.
    for (int i = 0; i < partInfo.getDefs().size(); i++) {
      TiPartitionDef pDef = partInfo.getDefs().get(i);
      String current = pDef.getLessThan().get(0);
      String leftHand;
      if (current.equals("MAXVALUE")) {
        leftHand = "true";
      } else {
        leftHand = String.format("%s < %s", partExprStr, current);
      }
      if (i == 0) {
        partExprs.add(parser.parseExpression(leftHand));
      } else {
        String previous = partInfo.getDefs().get(i - 1).getLessThan().get(0);
        String and;
        if (leftHand.equals("true")) {
          and = String.format("%s", partExprStr + ">=" + previous);
        } else {
          and = String.format("%s and %s", partExprStr + ">=" + previous, leftHand);
        }
        partExprs.add(parser.parseExpression(and));
      }
    }
    return partExprs;
  }
}
