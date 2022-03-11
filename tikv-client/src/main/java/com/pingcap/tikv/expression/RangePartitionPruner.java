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

import static com.pingcap.tikv.expression.PartitionPruner.extractLogicalOrComparisonExpr;

import com.google.common.collect.RangeSet;
import com.pingcap.tikv.exception.UnsupportedPartitionExprException;
import com.pingcap.tikv.exception.UnsupportedSyntaxException;
import com.pingcap.tikv.expression.visitor.PartAndFilterExprRewriter;
import com.pingcap.tikv.expression.visitor.PrunedPartitionBuilder;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.meta.TiPartitionDef;
import com.pingcap.tikv.meta.TiPartitionInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.parser.TiParser;
import com.pingcap.tikv.predicates.PredicateUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@SuppressWarnings("UnstableApiUsage")
public class RangePartitionPruner {
  private final TiPartitionInfo partInfo;
  private final Set<ColumnRef> partExprColRefs = new HashSet<>();
  private Expression partExpr;
  private List<Expression> partExprs;
  private PrunedPartitionBuilder rangeBuilder;
  private boolean foundUnsupportedPartExpr;

  RangePartitionPruner(TiTableInfo tableInfo) {
    this.partInfo = tableInfo.getPartitionInfo();
    try {
      this.partExprs = generateRangePartExprs(tableInfo);
      this.rangeBuilder = new PrunedPartitionBuilder(partExprColRefs);
    } catch (UnsupportedSyntaxException | UnsupportedPartitionExprException e) {
      foundUnsupportedPartExpr = true;
    }
  }

  private List<TiPartitionDef> pruneRangeNormalPart(Expression cnfExpr) {
    Objects.requireNonNull(cnfExpr, "cnf expression cannot be null at pruning stage");

    // we need rewrite filter expression if partition expression is a Year expression.
    // This step is designed to deal with y < '1995-10-10'(in filter condition and also a part of
    // partition expression) where y is a date type.
    // Rewriting only applies partition expression on the constant part, resulting year(y) < 1995.
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

    partExpr = parser.parseExpression(partExprStr);
    // when partExpr is null, it indicates partition expression
    // is not supported for now
    if (partExpr == null) {
      throw new UnsupportedPartitionExprException(
          String.format("%s is not supported", partExprStr));
    } // when it is not range column case, only first element stores useful info.

    partExprColRefs.addAll(PredicateUtils.extractColumnRefFromExpression(partExpr));
    PartitionPruner.generateRangeExprs(partInfo, partExprs, parser, partExprStr, 0);

    return partExprs;
  }

  /**
   * When table is a partition table and its type is range. We use this method to do the pruning.
   * Range partition has two types: 1. range 2. range column. If it is the first case,
   * pruneRangeNormalPart will be called. Otherwise pruneRangeColPart will be called. For now, we
   * simply skip range column partition case.
   *
   * @param filters is where condition belong to a select statement.
   * @return a pruned partition for scanning.
   */
  public List<TiPartitionDef> prune(List<Expression> filters) {
    filters = extractLogicalOrComparisonExpr(filters);
    Expression cnfExpr = PredicateUtils.mergeCNFExpressions(filters);
    if (!canBePruned(cnfExpr)) {
      return this.partInfo.getDefs();
    }

    return pruneRangeNormalPart(cnfExpr);
  }

  /**
   * return false if table cannot be pruning or partition table is not enabled. Return true if
   * partition pruning can be applied.
   *
   * @param filter is a where condition. It must be a cnf and does not contain not or isnull.
   * @return true if partition pruning can apply on filter.
   */
  public boolean canBePruned(Expression filter) {
    if (foundUnsupportedPartExpr) {
      return false;
    }
    // if query is select * from t, then filter will be null.
    return filter != null;
  }
}
