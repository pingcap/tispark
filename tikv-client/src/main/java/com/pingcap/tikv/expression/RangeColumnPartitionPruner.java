/*
 * Copyright 2020 PingCAP, Inc.
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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.pingcap.tikv.expression.ComparisonBinaryExpression.NormalizedPredicate;
import com.pingcap.tikv.expression.visitor.DefaultVisitor;
import com.pingcap.tikv.expression.visitor.PrunedPartitionBuilder;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.meta.TiPartitionDef;
import com.pingcap.tikv.meta.TiPartitionInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.parser.TiParser;
import com.pingcap.tikv.predicates.PredicateUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@SuppressWarnings("UnstableApiUsage")
public class RangeColumnPartitionPruner
    extends DefaultVisitor<Set<Integer>, LogicalBinaryExpression> {
  private final int partsSize;
  private final TiPartitionInfo partInfo;
  private final Map<String, List<Expression>> partExprsPerColumnRef;

  RangeColumnPartitionPruner(TiTableInfo tableInfo) {
    this.partExprsPerColumnRef = new HashMap<>();
    this.partInfo = tableInfo.getPartitionInfo();
    TiParser parser = new TiParser(tableInfo);
    for (int i = 0; i < partInfo.getColumns().size(); i++) {
      List<Expression> partExprs = new ArrayList<>();
      String colRefName = partInfo.getColumns().get(i);
      PartitionPruner.generateRangeExprs(partInfo, partExprs, parser, colRefName, i);
      partExprsPerColumnRef.put(colRefName, partExprs);
    }
    this.partsSize = tableInfo.getPartitionInfo().getDefs().size();
  }

  @Override
  protected Set<Integer> visit(LogicalBinaryExpression node, LogicalBinaryExpression parent) {
    Expression left = node.getLeft();
    Expression right = node.getRight();
    Set<Integer> partsIsCoveredByLeft = left.accept(this, node);
    Set<Integer> partsIsCoveredByRight = right.accept(this, node);
    switch (node.getCompType()) {
      case OR:
        partsIsCoveredByLeft.addAll(partsIsCoveredByRight);
        return partsIsCoveredByLeft;
      case AND:
        Set<Integer> partsIsCoveredByBoth = new HashSet<>();
        for (int i = 0; i < partsSize; i++) {
          if (partsIsCoveredByLeft.contains(i) && partsIsCoveredByRight.contains(i)) {
            partsIsCoveredByBoth.add(i);
          }
        }
        return partsIsCoveredByBoth;
    }

    throw new UnsupportedOperationException("cannot access here");
  }

  @Override
  protected Set<Integer> visit(ComparisonBinaryExpression node, LogicalBinaryExpression parent) {
    NormalizedPredicate predicate = node.normalize();
    if (predicate == null) {
      throw new UnsupportedOperationException(
          String.format("ComparisonBinaryExpression %s cannot be normalized", node.toString()));
    }
    String colRefName = predicate.getColumnRef().getName();
    List<Expression> partExprs = partExprsPerColumnRef.get(colRefName);
    Set<Integer> partDefs = new HashSet<>();
    if (partExprs == null) {
      switch (parent.getCompType()) {
        case OR:
          return partDefs;
        case AND:
          for (int i = 0; i < partsSize; i++) {
            partDefs.add(i);
          }
          return partDefs;
      }
    }
    Objects.requireNonNull(partExprs, "partition expression cannot be null");
    for (int i = 0; i < partsSize; i++) {
      PrunedPartitionBuilder rangeBuilder =
          new PrunedPartitionBuilder(ImmutableSet.of(predicate.getColumnRef()));
      RangeSet<TypedKey> partExprRange = rangeBuilder.buildRange(partExprs.get(i));
      RangeSet<TypedKey> filterRange = rangeBuilder.buildRange(node);
      RangeSet<TypedKey> copy = TreeRangeSet.create(partExprRange);
      copy.removeAll(filterRange.complement());
      // part expr and filter is connected
      if (!copy.isEmpty()) {
        partDefs.add(i);
      }
    }
    return partDefs;
  }

  public List<TiPartitionDef> prune(List<Expression> filters) {
    filters = extractLogicalOrComparisonExpr(filters);
    Expression cnfExpr = PredicateUtils.mergeCNFExpressions(filters);
    if (cnfExpr == null) {
      return partInfo.getDefs();
    }
    Set<Integer> partsIdx = cnfExpr.accept(this, null);
    List<TiPartitionDef> pDefs = new ArrayList<>();
    for (int i = 0; i < partsSize; i++) {
      if (partsIdx.contains(i)) {
        // part range is empty indicates this partition can be pruned.
        pDefs.add(partInfo.getDefs().get(i));
      }
    }
    return pDefs;
  }
}
