/*
 * Copyright 2023 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.expression;

import static com.pingcap.tikv.expression.PartitionPruner.extractLogicalOrComparisonExpr;

import com.pingcap.tikv.expression.visitor.PartAndFilterExprRewriter;
import com.pingcap.tikv.expression.visitor.PrunedPartitionBuilder;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.meta.TiPartitionDef;
import com.pingcap.tikv.meta.TiPartitionInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.parser.TiParser;
import com.pingcap.tikv.predicates.PredicateUtils;
import java.util.ArrayList;
import java.util.List;
import org.tikv.shade.com.google.common.collect.RangeSet;

public class ListPartitionPruner {

  private final TiPartitionInfo partInfo;
  private final Expression partExpr;
  private List<Expression> partExprs;
  private PrunedPartitionBuilder rangeBuilder;
  private boolean foundUnsupportedPartExpr = false;

  ListPartitionPruner(TiTableInfo tableInfo) {
    this.partInfo = tableInfo.getPartitionInfo();
    TiParser parser = new TiParser(tableInfo);
    String partExprStr = partInfo.getExpr();

    this.partExpr = parser.parseExpression(partExprStr);
    if (partExpr == null) {
      // add log here
      foundUnsupportedPartExpr = true;
      return;
    }

    this.partExprs = PartitionPruner.generateListExprs(partInfo, parser, partExprStr, 0);
    this.rangeBuilder =
        new PrunedPartitionBuilder(PredicateUtils.extractColumnRefFromExpression(partExpr));
  }

  public List<TiPartitionDef> prune(List<Expression> filters) {
    filters = extractLogicalOrComparisonExpr(filters);
    Expression cnfExpr = PredicateUtils.mergeCNFExpressions(filters);
    if (foundUnsupportedPartExpr || cnfExpr == null) {
      return this.partInfo.getDefs();
    }

    // we need rewrite filter expression if partition expression is a Year expression.
    // This step is designed to deal with y < '1995-10-10'(in filter condition and also a part of
    // partition expression) where y is a date type.
    // Rewriting only applies partition expression on the constant part, resulting y < 1995.
    PartAndFilterExprRewriter expressionRewriter = new PartAndFilterExprRewriter(partExpr);
    cnfExpr = expressionRewriter.rewrite(cnfExpr);
    // TODO
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
}
