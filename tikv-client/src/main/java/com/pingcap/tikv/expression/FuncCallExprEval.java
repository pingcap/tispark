package com.pingcap.tikv.expression;

import com.pingcap.tikv.expression.FuncCallExpr.Type;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DateTimeType;
import com.pingcap.tikv.types.DateType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.StringType;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.joda.time.DateTime;

public class FuncCallExprEval {

  private static final Map<Type, Function<Constant, Constant>> evalMap = new HashMap<>();

  static {
    // adding year eval logic here
    evalMap.put(
        Type.YEAR,
        literal -> {
          DataType type = literal.getDataType();
          if (type instanceof StringType) {
            DateTime date = DateTime.parse((String) literal.getValue());
            return Constant.create(date.getYear(), IntegerType.INT);
          } else if (type instanceof DateType) {
            DateTime date = (DateTime) literal.getValue();
            return Constant.create(date.getYear(), IntegerType.INT);
          } else if (type instanceof DateTimeType) {
            DateTime date = (DateTime) literal.getValue();
            return Constant.create(date.getYear(), IntegerType.INT);
          }
          throw new UnsupportedOperationException(
              String.format("cannot apply year to %s", type.getClass().getSimpleName()));
        });

    // for newly adding type, please also adds the corresponding logic here.
  }

  static Function<Constant, Constant> getEvalFn(Type tp) {
    if (evalMap.containsKey(tp)) {
      return evalMap.get(tp);
    }
    return null;
  }
}
