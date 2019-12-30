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

package com.pingcap.tikv.expression;

import com.google.common.collect.RangeSet;
import com.pingcap.tikv.exception.UnsupportedPartitionExprException;
import com.pingcap.tikv.exception.UnsupportedSyntaxException;
import com.pingcap.tikv.expression.visitor.PartAndFilterExprRewriter;
import com.pingcap.tikv.expression.visitor.PrunedPartitionBuilder;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.meta.TiPartitionDef;
import com.pingcap.tikv.meta.TiPartitionInfo;
import com.pingcap.tikv.meta.TiPartitionInfo.PartitionType;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.parser.TiParser;
import com.pingcap.tikv.predicates.PredicateUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@SuppressWarnings("UnstableApiUsage")
public class PartitionPruner {
  private Expression partExpr;
  private Set<ColumnRef> partExprColRefs = new HashSet<>();
  private List<Expression> partExprs;
  private PrunedPartitionBuilder rangeBuilder;
  private final TiPartitionInfo partInfo;
  private final PartitionType type;
  private boolean isRangeCol = false;
  private final boolean partitionEnabled;
  private boolean foundUnsupportedPartExpr;

  public PartitionPruner(TiTableInfo tableInfo) {
    this.type = TiPartitionInfo.toPartType((int) tableInfo.getPartitionInfo().getType());
    this.partInfo = tableInfo.getPartitionInfo();
    this.partitionEnabled = tableInfo.isPartitionEnabled();
    List<String> columnInfos = this.partInfo.getColumns();
    if (this.type == PartitionType.RangePartition) {
      this.isRangeCol = Objects.requireNonNull(columnInfos).size() > 0;
      try {
        this.partExprs = generateRangePartExprs(tableInfo);
        this.rangeBuilder = new PrunedPartitionBuilder(partExprColRefs);
      } catch (UnsupportedSyntaxException | UnsupportedPartitionExprException e) {
        foundUnsupportedPartExpr = true;
      }
    }
  }

  // pruneListPart will prune list partition that where conditions never access.
  private List<TiPartitionDef> pruneListPart(List<Expression> filters) {
    return partInfo.getDefs();
  }

  // pruneHashPart will prune hash partition that where conditions never access.
  private List<TiPartitionDef> pruneHashPart(List<Expression> filters) {
    return partInfo.getDefs();
  }

  private List<TiPartitionDef> pruneRangeColumnPart(Expression cnfExpr) {
    RangeSet<TypedKey> filterRange = rangeBuilder.buildRange(cnfExpr);

    List<TiPartitionDef> pDefs = new ArrayList<>();
    for (int i = 0; i < partExprs.size(); i++) {
      Expression partExpr = partExprs.get(i);
      // when we build range, we still need rewrite partition expression.
      // If we have a year(purchased) < 1995 which cannot be normalized, we need
      // to rewrite it into purchased < 1995 to let RangeSetBuilder be happy.
      RangeSet<TypedKey> partRange = rangeBuilder.buildRange(partExpr);
      partRange.removeAll(filterRange.complement());
      if (!partRange.isEmpty()) {
        // part range is empty indicates this partition can be pruned.
        pDefs.add(partInfo.getDefs().get(i));
      }
    }
    return pDefs;
  }

  private List<TiPartitionDef> pruneRangeNormalPart(Expression cnfExpr) {
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
    RangeSet<TypedKey> filterRange = rangeBuilder.buildRange(cnfExpr);

    List<TiPartitionDef> pDefs = new ArrayList<>();
    for (int i = 0; i < partExprs.size(); i++) {
      Expression partExpr = partExprs.get(i);
      // when we build range, we still need rewrite partition expression.
      // If we have a year(purchased) < 1995 which cannot be normalized, we need
      // to rewrite it into purchased < 1995 to let RangeSetBuilder be happy.
      RangeSet<TypedKey> partRange = rangeBuilder.buildRange(expressionRewriter.rewrite(partExpr));
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
   * @param filters is where condition belong to a select statement.
   * @return a pruned partition for scanning.
   */
  // pruneRangePart will prune range partition that where conditions never access.
  private List<TiPartitionDef> pruneRangePart(List<Expression> filters) {
    filters = extractLogicalOrComparisonExpr(filters);
    Expression cnfExpr = PredicateUtils.mergeCNFExpressions(filters);
    if (isRangeCol) {
      return pruneRangeColumnPart(cnfExpr);
    } else {
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
      if (!canBePruned(cnfExpr)) {
        return this.partInfo.getDefs();
      }

      return pruneRangeNormalPart(cnfExpr);
    }
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
  private List<Expression> generateRangePartExprs(TiTableInfo tableInfo) {
    TiPartitionInfo partInfo = tableInfo.getPartitionInfo();
    List<Expression> partExprs = new ArrayList<>();
    TiParser parser = new TiParser(tableInfo);
    // check year expression
    // rewrite filter condition
    // purchased > '1995-10-10'
    // year(purchased) > year('1995-10-10')
    // purchased > 1995
    String partExprStr = tableInfo.getPartitionInfo().getExpr();
    if (!partExprStr.isEmpty()) {
      // when it is not range column case, only first element stores useful info.
      generateRangeExprs(partInfo, partExprs, parser, partExprStr, 0);
    } else {
      for (int i = 0; i < partInfo.getColumns().size(); i++) {
        generateRangeExprs(partInfo, partExprs, parser, partInfo.getColumns().get(i), i);
      }
    }

    return partExprs;
  }

  private void generateRangeExprs(
      TiPartitionInfo partInfo,
      List<Expression> partExprs,
      TiParser parser,
      String partExprStr,
      int lessThanIdx) {
    partExpr = parser.parseExpression(partExprStr);
    // when partExpr is null, it indicates partition expression
    // is not supported for now
    if (partExpr == null) {
      throw new UnsupportedPartitionExprException(
          String.format("%s is not supported", partExprStr));
    }
    partExprColRefs.addAll(PredicateUtils.extractColumnRefFromExpression(partExpr));
    for (int i = 0; i < partInfo.getDefs().size(); i++) {
      TiPartitionDef pDef = partInfo.getDefs().get(i);
      String current = pDef.getLessThan().get(lessThanIdx);
      String leftHand;
      if (current.equals("MAXVALUE")) {
        leftHand = "true";
      } else {
        leftHand = String.format("%s < %s", partExprStr, current);
      }
      if (i == 0) {
        partExprs.add(parser.parseExpression(leftHand));
      } else {
        String previous = partInfo.getDefs().get(i - 1).getLessThan().get(lessThanIdx);
        String and = String.format("%s and %s", partExprStr + ">=" + previous, leftHand);
        partExprs.add(parser.parseExpression(and));
      }
    }
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

  public List<TiPartitionDef> prune(List<Expression> filters) {
    switch (type) {
      case RangePartition:
        return pruneRangePart(filters);
      case ListPartition:
        return pruneListPart(filters);
      case HashPartition:
        return pruneHashPart(filters);
    }

    throw new UnsupportedOperationException("cannot prune under invalid partition table");
  }

  /**
   * return false if table cannot be pruning or partition table is not enabled. Return true if
   * partition pruning can be applied.
   *
   * @param filter is a where condition. It must be a cnf and does not contain not or isnull.
   * @return true if partition pruning can apply on filter.
   */
  public boolean canBePruned(Expression filter) {
    if (!partitionEnabled) {
      return false;
    }

    if (this.type != PartitionType.RangePartition) {
      return false;
    }

    if (foundUnsupportedPartExpr) {
      return false;
    }
    // if query is select * from t, then filter will be null.
    return filter != null;
  }
}
