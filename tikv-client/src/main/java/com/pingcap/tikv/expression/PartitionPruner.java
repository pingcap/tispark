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

import com.pingcap.tikv.meta.TiPartitionDef;
import com.pingcap.tikv.meta.TiPartitionInfo;
import com.pingcap.tikv.meta.TiPartitionInfo.PartitionType;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.parser.TiParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PartitionPruner {
  public static List<Expression> extractLogicalOrComparisonExpr(List<Expression> filters) {
    List<Expression> filteredFilters = new ArrayList<>();
    for (Expression expr : filters) {
      if (expr instanceof LogicalBinaryExpression || expr instanceof ComparisonBinaryExpression) {
        filteredFilters.add(expr);
      }
    }
    return filteredFilters;
  }

  /**
   * When table is a partition table and its type is range. We use this method to do the pruning.
   * Range partition has two types: 1. RANGE partitioning 2. RANGE COLUMNS partitioning
   * https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-range.html
   * https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-columns-range.html
   *
   * @param filters is where condition belong to a select statement.
   * @return a pruned partition for scanning.
   */
  public static List<TiPartitionDef> prune(TiTableInfo tableInfo, List<Expression> filters) {
    PartitionType type = tableInfo.getPartitionInfo().getType();
    if (!tableInfo.isPartitionEnabled()) {
      return tableInfo.getPartitionInfo().getDefs();
    }

    boolean isRangeColPartitioning =
        Objects.requireNonNull(tableInfo.getPartitionInfo().getColumns()).size() > 0;

    switch (type) {
      case RangePartition:
        if (!isRangeColPartitioning) {
          RangePartitionPruner pruner = new RangePartitionPruner(tableInfo);
          return pruner.prune(filters);
        } else {
          // For a table partitioned by RANGE COLUMNS, currently TiDB only supports using a single
          // partitioning column.
          // So currently we only support prune with a single partitioning column.
          // If we meet range partition on multiple columns(maybe TiDB support in future), we simply
          // return all parts.
          if (tableInfo.getPartitionInfo().getColumns().size() > 1) {
            return tableInfo.getPartitionInfo().getDefs();
          }

          RangeColumnPartitionPruner pruner = new RangeColumnPartitionPruner(tableInfo);
          return pruner.prune(filters);
        }
      case ListPartition:
      case HashPartition:
        return tableInfo.getPartitionInfo().getDefs();
    }

    throw new UnsupportedOperationException("cannot prune under invalid partition table");
  }

  static void generateRangeExprs(
      TiPartitionInfo partInfo,
      List<Expression> partExprs,
      TiParser parser,
      String partExprStr,
      int lessThanIdx) {
    // partExprColRefs.addAll(PredicateUtils.extractColumnRefFromExpression(partExpr));
    for (int i = 0; i < partInfo.getDefs().size(); i++) {
      TiPartitionDef pDef = partInfo.getDefs().get(i);
      String current = pDef.getLessThan().get(lessThanIdx);
      String leftHand;
      if (current.equals("MAXVALUE")) {
        leftHand = "true";
      } else {
        leftHand = String.format("%s < %s", wrapColumnName(partExprStr), current);
      }
      if (i == 0) {
        partExprs.add(parser.parseExpression(leftHand));
      } else {
        String previous = partInfo.getDefs().get(i - 1).getLessThan().get(lessThanIdx);
        String and =
            String.format("%s >= %s and %s", wrapColumnName(partExprStr), previous, leftHand);
        partExprs.add(parser.parseExpression(and));
      }
    }
  }

  private static String wrapColumnName(String columnName) {
    if (columnName.startsWith("`") && columnName.endsWith("`")) {
      return columnName;
    } else if (columnName.contains("(") && columnName.contains(")")) {
      // function not column name, e.g. year(columnName)
      return columnName;
    } else {
      return String.format("`%s`", columnName);
    }
  }
}
