package com.pingcap.tikv.expression.aggregate;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.Types;

public class Count extends TiFunctionExpression {

  public Count(TiExpr... arg) {
    super(arg);
  }

  @Override
  protected ExprType getExprType() {
    return ExprType.Count;
  }

  @Override
  public DataType getType() {
    return DataTypeFactory.of(Types.TYPE_LONG);
  }
}
