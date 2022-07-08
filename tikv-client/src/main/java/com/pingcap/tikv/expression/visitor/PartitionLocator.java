package com.pingcap.tikv.expression.visitor;

import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.ComparisonBinaryExpression.Operator;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.partition.PartitionedTable.PartitionLocatorContext;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;

public class PartitionLocator extends DefaultVisitor<Boolean, PartitionLocatorContext> {

  @Override
  public Boolean visit(ComparisonBinaryExpression node, PartitionLocatorContext context) {
    ColumnRef columnRef = (ColumnRef) node.getLeft();
    Constant constant = (Constant) node.getRight();
    columnRef.resolve(context.getTableInfo());
    Row row = context.getRow();
    DataType type = columnRef.getColumnInfo().getType();
    Object data = row.get(columnRef.getColumnInfo().getOffset(), type);
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
          String.format(
              ("Unsupported constant, type: %s, value: %s\n"),
              node.getDataType(),
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
