package com.pingcap.tikv.expression.aggregate;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiUnaryFunctionExpression;
import com.pingcap.tikv.types.DataType;

public class Min extends TiUnaryFunctionExpression {

  public Min(TiExpr arg) {
    super(arg);
  }

  @Override
  protected ExprType getExprType() {
    return ExprType.Min;
  }

  @Override
  public DataType getType() {
    return args.get(0).getType();
  }
}
