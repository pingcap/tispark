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
    PartitionExpression partitionExpr = new PartitionExpression();
    switch (partitionInfo.getType()) {
      case RangePartition:
        if (partitionInfo.getColumns().isEmpty()) {
          partitionExpr.setRangePartitionExpressions(generateRangePartitionExpr(tableInfo));
        } else {
          partitionExpr.setRangeColumnRefExpressions(generateRangeColumnPartitionExpr(tableInfo));
        }
        break;
      case HashPartition:
        partitionExpr.setHashPartitionExpressions(generateHashPartitionExpr(tableInfo));
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported partition type %s", partitionInfo.getType()));
    }

    return partitionExpr;
  }

  private static Map<String, List<Expression>> generateRangeColumnPartitionExpr(
      TiTableInfo tableInfo) {
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

    return column2PartitionExps;
  }

  private static Expression generateHashPartitionExpr(TiTableInfo tableInfo) {
    TiParser parser = new TiParser(tableInfo);
    return parser.parseExpression(tableInfo.getPartitionInfo().getExpr());
  }

  private static List<Expression> generateRangePartitionExpr(TiTableInfo tableInfo) {
    TiPartitionInfo partitionInfo = tableInfo.getPartitionInfo();
    String partitionExpr = partitionInfo.getExpr();
    TiParser parser = new TiParser(tableInfo);

    List<Expression> rangePartitionExps = new ArrayList<>();
    PartitionPruner.generateRangeExprs(partitionInfo, rangePartitionExps, parser, partitionExpr, 0);
    return rangePartitionExps;
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
    Expression hashPartitionExpressions =
        Objects.requireNonNull(
            partitionExpr.getHashPartitionExpressions(),
            "HashPartitionExpression should not be null");

    if (hashPartitionExpressions instanceof ColumnRef) {
      ColumnRef columnRef = (ColumnRef) hashPartitionExpressions;
      columnRef.resolve(logicalTable.getTableInfo());
      Number id =
          (Number)
              row.get(columnRef.getColumnInfo().getOffset(), columnRef.getColumnInfo().getType());
      int partitionId = (int) (id.longValue() % physicalTables.length);
      return physicalTables[partitionId];
    } else if (hashPartitionExpressions instanceof FuncCallExpr) {
      // TODO: support more function partition
      FuncCallExpr partitionFuncExpr = (FuncCallExpr) hashPartitionExpressions;
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

  private TableCommon locateRangeColumnPartition(Row data) {
    Map<String, List<Expression>> rangeColumnRefExpressions =
        partitionExpr.getRangeColumnRefExpressions();
    if (rangeColumnRefExpressions.size() != 1) {
      throw new UnsupportedOperationException(
          "Currently only support range column partition on a single column");
    }

    PartitionLocator partitionLocator = new PartitionLocator();

    for (Entry<String, List<Expression>> entry : rangeColumnRefExpressions.entrySet()) {
      List<Expression> value = entry.getValue();
      for (int i = 0; i < value.size(); i++) {
        Expression expression = value.get(i);
        Boolean accept =
            expression.accept(
                partitionLocator, new PartitionLocatorContext(logicalTable.getTableInfo(), data));
        if (accept) {
          return physicalTables[i];
        }
      }
    }

    throw new IllegalArgumentException("Cannot find partition for row " + data);
  }

  private TableCommon locateRangePartition(Row data) {
    return null;
  }

  @Data
  public static class PartitionLocatorContext {
    private final TiTableInfo tableInfo;
    private final Row row;
  }
}
