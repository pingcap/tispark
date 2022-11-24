/*
 * Copyright 2020 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.joda.time.Days;

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
            // literal can be java.sql.Date, use new DateTime() to avoid convert error
            DateTime date = new DateTime(literal.getValue());
            return Constant.create(date.getYear(), IntegerType.INT);
          } else if (type instanceof DateTimeType) {
            // literal can be java.sql.Timestamp, use new DateTime() to avoid convert error
            DateTime date = new DateTime(literal.getValue());
            return Constant.create(date.getYear(), IntegerType.INT);
          }
          throw new UnsupportedOperationException(
              String.format("cannot apply year on %s", type.getName()));
        });

    evalMap.put(
        Type.TO_DAYS,
        literal -> {
          DataType type = literal.getDataType();
          if (type instanceof StringType) {
            DateTime date = DateTime.parse((String) literal.getValue());
            return Constant.create(dateTime2ToDays(date), IntegerType.INT);
          } else if (type instanceof DateType) {
            DateTime date = new DateTime(literal.getValue());
            return Constant.create(dateTime2ToDays(date), IntegerType.INT);
          } else if (type instanceof DateTimeType) {
            DateTime date = new DateTime(literal.getValue());
            return Constant.create(dateTime2ToDays(date), IntegerType.INT);
          }
          throw new UnsupportedOperationException(
              String.format("cannot apply to_days on %s", type.getName()));
        });

    // for newly adding type, please also adds the corresponding logic here.
  }

  static Function<Constant, Constant> getEvalFn(Type tp) {
    if (evalMap.containsKey(tp)) {
      return evalMap.get(tp);
    }
    return null;
  }

  private static int dateTime2ToDays(DateTime date) {
    DateTime start = DateTime.parse("0000-01-01");
    return Days.daysBetween(start.toLocalDate(), date.toLocalDate()).getDays();
  }
}
