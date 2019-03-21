package com.pingcap.tikv.expression;

import com.pingcap.tikv.expression.FuncCallExpr.Type;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DateTimeType;
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
          DataType type = literal.getType();
          if (type == StringType.VARCHAR) {
            DateTime date = DateTime.parse((String) literal.getValue());
            return Constant.create(date.getYear());
          } else if (type == DateTimeType.DATETIME) {
            DateTime date = (DateTime) literal.getValue();
            return Constant.create(date.getYear());
          } else {
            return literal;
          }
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
