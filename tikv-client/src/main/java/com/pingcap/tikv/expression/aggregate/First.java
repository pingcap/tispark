package com.pingcap.tikv.expression.aggregate;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiUnaryFunctionExpression;
import com.pingcap.tikv.types.DataType;

public class First extends TiUnaryFunctionExpression {
  public First(TiExpr arg) {
    super(arg);
  }

  @Override
  protected ExprType getExprType() {
    return ExprType.First;
  }

  @Override
  public DataType getType() {
    return args.get(0).getType();
  }
}
