/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.partition;

import static com.pingcap.tikv.expression.FuncCallExpr.Type.YEAR;

import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.FuncCallExpr;
import com.pingcap.tikv.expression.PartitionPruner;
import com.pingcap.tikv.expression.visitor.PartitionLocator;
import com.pingcap.tikv.meta.TiPartitionDef;
import com.pingcap.tikv.meta.TiPartitionInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.parser.TiParser;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DateType;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class PartitionedTable implements Serializable {

  private static final PartitionLocator partitionLocator = new PartitionLocator();

  private final TableCommon logicalTable;

  private final TableCommon[] physicalTables;

  private final PartitionExpression partitionExpr;

  private final TiPartitionInfo partitionInfo;

  public static PartitionedTable newPartitionTable(
      TableCommon logicalTable, TiTableInfo tableInfo) {
    PartitionExpression partitionExpr = generatePartitionExpr(tableInfo);

    List<TiPartitionDef> partitionDefs = tableInfo.getPartitionInfo().getDefs();
    TableCommon[] physicalTables = new TableCommon[partitionDefs.size()];
    for (int i = 0; i < partitionDefs.size(); i++) {
      TiPartitionDef tiPartitionDef = partitionDefs.get(i);
      TableCommon temp =
          new TableCommon(
              logicalTable.getLogicalTableId(),
              tiPartitionDef.getId(),
              logicalTable.getTableInfo());
      physicalTables[i] = temp;
    }

    return new PartitionedTable(
        logicalTable, physicalTables, partitionExpr, tableInfo.getPartitionInfo());
  }

  public static PartitionExpression generatePartitionExpr(TiTableInfo tableInfo) {
    TiPartitionInfo partitionInfo = tableInfo.getPartitionInfo();
    switch (partitionInfo.getType()) {
      case RangePartition:
        if (partitionInfo.getColumns().isEmpty()) {
          return generateRangePartitionExpr(tableInfo);
        } else {
          return generateRangeColumnPartitionExpr(tableInfo);
        }
      case HashPartition:
        return generateHashPartitionExpr(tableInfo);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported partition type %s", partitionInfo.getType()));
    }
  }

  private static PartitionExpression generateRangeColumnPartitionExpr(TiTableInfo tableInfo) {
    PartitionExpression partitionExpr = new PartitionExpression();
    TiPartitionInfo partitionInfo = tableInfo.getPartitionInfo();
    if (partitionInfo.getColumns().size() > 1) {
      throw new UnsupportedOperationException(
          "Currently only support partition on a single column");
    }

    Map<String, List<Expression>> column2PartitionExps = new HashMap<>();
    TiParser parser = new TiParser(tableInfo);
    for (int i = 0; i < partitionInfo.getColumns().size(); i++) {
      List<Expression> partExprs = new ArrayList<>();
      String colRefName = partitionInfo.getColumns().get(i);
      PartitionPruner.generateRangeExprs(partitionInfo, partExprs, parser, colRefName, i);
      column2PartitionExps.put(colRefName, partExprs);
    }
    partitionExpr.setRangeColumnRefBoundExpressions(column2PartitionExps);

    return partitionExpr;
  }

  private static PartitionExpression generateHashPartitionExpr(TiTableInfo tableInfo) {
    TiParser parser = new TiParser(tableInfo);
    PartitionExpression partitionExpr = new PartitionExpression();
    partitionExpr.setOriginExpression(
        parser.parseExpression(tableInfo.getPartitionInfo().getExpr()));

    return partitionExpr;
  }

  private static PartitionExpression generateRangePartitionExpr(TiTableInfo tableInfo) {
    PartitionExpression partitionExpr = new PartitionExpression();
    TiPartitionInfo partitionInfo = tableInfo.getPartitionInfo();
    String originExpr = partitionInfo.getExpr();
    TiParser parser = new TiParser(tableInfo);
    partitionExpr.setOriginExpression(parser.parseExpression(originExpr));

    List<Expression> rangePartitionExps = new ArrayList<>();
    PartitionPruner.generateRangeExprs(partitionInfo, rangePartitionExps, parser, originExpr, 0);
    partitionExpr.setRangePartitionBoundExpressions(rangePartitionExps);

    return partitionExpr;
  }

  public TableCommon locatePartition(Row row) {
    switch (partitionInfo.getType()) {
      case RangePartition:
        if (partitionInfo.getColumns().isEmpty()) {
          return locateRangePartition(row);
        } else {
          return locateRangeColumnPartition(row);
        }
      case HashPartition:
        return locateHashPartition(row);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported partition type %s", partitionInfo.getType()));
    }
  }

  private TableCommon locateHashPartition(Row row) {
    Expression originalExpr =
        Objects.requireNonNull(
            partitionExpr.getOriginExpression(), "originalExpression should not be null");

    if (originalExpr instanceof ColumnRef) {
      ColumnRef columnRef = (ColumnRef) originalExpr;
      columnRef.resolve(logicalTable.getTableInfo());
      Number id =
          (Number)
              row.get(columnRef.getColumnInfo().getOffset(), columnRef.getColumnInfo().getType());
      int partitionId = (int) (id.longValue() % physicalTables.length);
      return physicalTables[partitionId];
    } else if (originalExpr instanceof FuncCallExpr) {
      // TODO: support more function partition
      FuncCallExpr partitionFuncExpr = (FuncCallExpr) originalExpr;
      if (partitionFuncExpr.getFuncTp() == YEAR) {
        int result =
            (int) partitionFuncExpr.eval(Constant.create(row.getDate(0), DateType.DATE)).getValue();
        int partitionId = result % physicalTables.length;
        return physicalTables[partitionId];
      } else {
        throw new UnsupportedOperationException(
            "Hash partition write only support YEAR() function");
      }
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported partition expr %s", partitionExpr));
    }
  }

  private TableCommon locateRangeColumnPartition(Row row) {
    Map<String, List<Expression>> rangeColumnRefExpressions =
        Objects.requireNonNull(
            partitionExpr.getRangeColumnRefBoundExpressions(),
            "RangeColumnRefBoundExpressions should not be null");
    if (rangeColumnRefExpressions.size() != 1) {
      throw new UnsupportedOperationException(
          "Currently only support range column partition on a single column");
    }

    int partitionIndex = -1;
    for (Entry<String, List<Expression>> entry : rangeColumnRefExpressions.entrySet()) {
      List<Expression> value = entry.getValue();
      partitionIndex = getPartitionIndex(row, value);
    }

    return physicalTables[partitionIndex];
  }

  private TableCommon locateRangePartition(Row row) {
    Expression originalExpr =
        Objects.requireNonNull(
            partitionExpr.getOriginExpression(), "originalExpression should not be null");
    List<Expression> rangePartitionBoundExpressions =
        Objects.requireNonNull(
            partitionExpr.getRangePartitionBoundExpressions(),
            "RangePartitionBoundExpressions should not be null");

    int partitionIndex = -1;

    if (originalExpr instanceof ColumnRef) {
      ColumnRef columnRef = (ColumnRef) originalExpr;
      columnRef.resolve(logicalTable.getTableInfo());
      partitionIndex = getPartitionIndex(row, rangePartitionBoundExpressions);
    } else if (originalExpr instanceof FuncCallExpr) {
      // TODO: support more function partition
      FuncCallExpr partitionFuncExpr = (FuncCallExpr) originalExpr;
      if (partitionFuncExpr.getFuncTp() == YEAR) {
        partitionIndex = getPartitionIndex(row, rangePartitionBoundExpressions);
      } else {
        throw new UnsupportedOperationException(
            "Range partition write only support YEAR() function");
      }
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported partition expr %s", partitionExpr));
    }

    return physicalTables[partitionIndex];
  }

  private int getPartitionIndex(Row row, List<Expression> rangePartitionBoundExpressions) {
    for (int i = 0; i < rangePartitionBoundExpressions.size(); i++) {
      Expression expression = rangePartitionBoundExpressions.get(i);
      Boolean accept =
          expression.accept(
              partitionLocator, new PartitionLocatorContext(logicalTable.getTableInfo(), row));
      if (accept) {
        return i;
      }
    }

    throw new IllegalArgumentException("Cannot find partition for row " + row);
  }

  @Data
  public static class PartitionLocatorContext {

    private final TiTableInfo tableInfo;
    private final Row row;
  }
}
