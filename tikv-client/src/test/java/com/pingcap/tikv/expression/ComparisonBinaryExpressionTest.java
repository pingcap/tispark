/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.expression;

import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.divide;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.NormalizedPredicate;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.Operator;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.equal;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.greaterEqual;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.greaterThan;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.lessEqual;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.lessThan;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.notEqual;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.StringType;
import org.junit.Test;

public class ComparisonBinaryExpressionTest {
  private static TiTableInfo createTable() {
    return new MetaUtils.TableBuilder()
        .name("testTable")
        .addColumn("c1", IntegerType.INT, true)
        .addColumn("c2", StringType.VARCHAR)
        .addColumn("c3", StringType.VARCHAR)
        .addColumn("c4", IntegerType.INT)
        .appendIndex("testIndex", ImmutableList.of("c1", "c2"), false)
        .build();
  }

  private void verifyNormalize(
      ComparisonBinaryExpression cond,
      String colName,
      Object value,
      DataType dataType,
      Operator operator) {
    NormalizedPredicate normCond = cond.normalize();
    assertEquals(colName, normCond.getColumnRef().getName());
    assertEquals(value, normCond.getValue().getValue());
    assertEquals(TypedKey.toTypedKey(value, dataType), normCond.getTypedLiteral());
    assertEquals(operator, normCond.getType());
  }

  @Test
  public void normalizeTest() {
    TiTableInfo table = createTable();
    ColumnRef col1 = ColumnRef.create("c1", table);
    Constant c1 = Constant.create(1, IntegerType.INT);
    // index col = c1, long
    ComparisonBinaryExpression cond = equal(col1, c1);
    verifyNormalize(cond, "c1", 1, IntegerType.INT, Operator.EQUAL);

    cond = lessEqual(c1, col1);
    verifyNormalize(cond, "c1", 1, IntegerType.INT, Operator.GREATER_EQUAL);

    cond = lessThan(c1, col1);
    verifyNormalize(cond, "c1", 1, IntegerType.INT, Operator.GREATER_THAN);

    cond = greaterEqual(c1, col1);
    verifyNormalize(cond, "c1", 1, IntegerType.INT, Operator.LESS_EQUAL);

    cond = greaterThan(c1, col1);
    verifyNormalize(cond, "c1", 1, IntegerType.INT, Operator.LESS_THAN);

    cond = equal(c1, col1);
    verifyNormalize(cond, "c1", 1, IntegerType.INT, Operator.EQUAL);

    cond = notEqual(c1, col1);
    verifyNormalize(cond, "c1", 1, IntegerType.INT, Operator.NOT_EQUAL);

    cond = lessEqual(col1, c1);
    verifyNormalize(cond, "c1", 1, IntegerType.INT, Operator.LESS_EQUAL);

    cond = equal(divide(col1, c1), c1);
    assertNull(cond.normalize());
  }
}
