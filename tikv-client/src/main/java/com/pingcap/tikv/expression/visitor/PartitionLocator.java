package com.pingcap.tikv.expression.visitor;

import static com.pingcap.tikv.expression.FuncCallExpr.Type.YEAR;

import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.ComparisonBinaryExpression.Operator;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.FuncCallExpr;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.partition.PartitionedTable.PartitionLocatorContext;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DateType;
import com.pingcap.tikv.types.IntegerType;

public class PartitionLocator extends DefaultVisitor<Boolean, PartitionLocatorContext> {

  @Override
  public Boolean visit(ComparisonBinaryExpression node, PartitionLocatorContext context) {
    Object data;
    Row row = context.getRow();
    TiTableInfo tableInfo = context.getTableInfo();
    Expression left = node.getLeft();
    if (left instanceof ColumnRef) {
      ColumnRef columnRef = (ColumnRef) left;
      columnRef.resolve(tableInfo);
      data = row.get(columnRef.getColumnInfo().getOffset(), columnRef.getColumnInfo().getType());
    } else if (left instanceof FuncCallExpr) {
      // TODO: support more function partition
      FuncCallExpr partitionFuncExpr = (FuncCallExpr) left;
      if (partitionFuncExpr.getFuncTp() == YEAR) {
        data = partitionFuncExpr.eval(Constant.create(row.getDate(0), DateType.DATE)).getValue();
      } else {
        throw new UnsupportedOperationException("Partition write only support YEAR() function");
      }
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported expr %s", left));
    }

    Constant constant = (Constant) node.getRight();
    Operator comparisonType = node.getComparisonType();

    switch (comparisonType) {
      case GREATER_EQUAL:
        return data.toString().compareTo(constant.getValue().toString()) >= 0;
      case LESS_THAN:
        return data.toString().compareTo(constant.getValue().toString()) < 0;
      default:
        throw new UnsupportedOperationException("Unsupported comparison type: " + comparisonType);
    }
  }

  @Override
  public Boolean visit(Constant node, PartitionLocatorContext context) {
    if (node.getDataType() == IntegerType.TINYINT) {
      return (int) node.getValue() == 1;
    } else {
      throw new IllegalStateException(
          String.format(("Unsupported constant, type: %s, value: %s\n"), node.getDataType(),
              node.getValue()));
    }
  }

  @Override
  public Boolean visit(LogicalBinaryExpression node, PartitionLocatorContext context) {
    Expression left = node.getLeft();
    Expression right = node.getRight();

    return left.accept(this, context) && right.accept(this, context);
  }
}
