package com.pingcap.tikv.expression.aggregate;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiUnaryFunctionExpression;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.DecimalType;
import com.pingcap.tikv.types.Types;
import org.apache.spark.sql.types.IntegerType;

public class Sum extends TiUnaryFunctionExpression {

  public Sum(TiExpr arg) {
    super(arg);
  }

  @Override
  protected ExprType getExprType() {
    return ExprType.Sum;
  }

  @Override
  public DataType getType() {
    // get column type from Column Reference
    DataType colType = args.get(0).getType();
    if(colType instanceof DecimalType || colType instanceof IntegerType) {
      return DataTypeFactory.of(Types.TYPE_NEW_DECIMAL);
    }
    // both Real and Double are decoded using RealType
    return DataTypeFactory.of(Types.TYPE_DOUBLE);
  }
}
