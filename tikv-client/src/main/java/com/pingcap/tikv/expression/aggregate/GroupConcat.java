package com.pingcap.tikv.expression.aggregate;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiUnaryFunctionExpression;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.Types;

public class GroupConcat extends TiUnaryFunctionExpression {

  public GroupConcat(TiExpr arg) {
    super(arg);
  }

  @Override
  protected ExprType getExprType() {
    return ExprType.GroupConcat;
  }

  @Override
  public DataType getType() {
    return DataTypeFactory.of(Types.TYPE_VARCHAR);
  }
}
