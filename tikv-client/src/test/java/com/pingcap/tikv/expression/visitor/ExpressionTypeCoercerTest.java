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

package com.pingcap.tikv.expression.visitor;

import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.minus;
import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.plus;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.equal;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.lessThan;
import static com.pingcap.tikv.expression.LogicalBinaryExpression.and;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.expression.ArithmeticBinaryExpression;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.RealType;
import com.pingcap.tikv.types.StringType;
import com.pingcap.tikv.types.TimestampType;
import java.util.Map;
import org.junit.Test;

public class ExpressionTypeCoercerTest {
  private static TiTableInfo createTable() {
    return new MetaUtils.TableBuilder()
        .name("testTable")
        .addColumn("c1", IntegerType.INT, true)
        .addColumn("c2", StringType.VARCHAR)
        .addColumn("c3", TimestampType.TIMESTAMP)
        .addColumn("c4", RealType.DOUBLE)
        .appendIndex("testIndex", ImmutableList.of("c1", "c2"), false)
        .build();
  }

  @Test
  public void typeVerifyWithColumnRefTest() throws Exception {
    TiTableInfo table = createTable();
    ColumnRef col1 = ColumnRef.create("c1", table); // INT
    ColumnRef col4 = ColumnRef.create("c4", table); // DOUBLE
    Constant c1 = Constant.create(1);
    Constant c2 = Constant.create(11.1);
    Constant c3 = Constant.create(11.1);
    Constant c4 = Constant.create(1.1);

    ArithmeticBinaryExpression ar1 = minus(col1, c1);
    ArithmeticBinaryExpression ar2 = plus(col4, c4);
    ComparisonBinaryExpression comp1 = equal(ar1, c2);
    ComparisonBinaryExpression comp2 = equal(ar2, c3);
    ComparisonBinaryExpression comp3 = equal(ar1, ar2);
    LogicalBinaryExpression log1 = and(comp1, comp2);
    LogicalBinaryExpression log2 = and(comp1, comp3);

    ExpressionTypeCoercer inf = new ExpressionTypeCoercer();
    assertEquals(IntegerType.BOOLEAN, inf.infer(log1));
    Map<Expression, DataType> map = inf.getTypeMap();
    assertEquals(IntegerType.INT, map.get(col1));
    assertEquals(IntegerType.INT, map.get(c1));
    assertEquals(IntegerType.INT, map.get(ar1));
    assertEquals(IntegerType.BOOLEAN, map.get(comp1));
    assertEquals(RealType.DOUBLE, map.get(col4));
    assertEquals(RealType.DOUBLE, map.get(c4));
    assertEquals(RealType.DOUBLE, map.get(ar2));
    assertEquals(IntegerType.BOOLEAN, map.get(comp2));

    inf = new ExpressionTypeCoercer();
    try {
      inf.infer(log2);
      fail();
    } catch (Exception e) {}
  }

  @Test
  public void typeVerifyTest() throws Exception {
    Constant const1 = Constant.create(1);
    Constant const2 = Constant.create(11);
    ComparisonBinaryExpression comp1 = equal(const1, const2);

    Constant const3 = Constant.create(1.1f);
    Constant const4 = Constant.create(1.111f);
    ComparisonBinaryExpression comp2 = lessThan(const3, const4);

    Constant const5 = Constant.create(1);
    Constant const6 = Constant.create(1.1f);
    ArithmeticBinaryExpression comp3 = minus(const5, const6);

    LogicalBinaryExpression and1 = and(comp1, comp2);
    LogicalBinaryExpression or1 = LogicalBinaryExpression.or(comp1, comp3);

    ExpressionTypeCoercer inf = new ExpressionTypeCoercer();
    assertEquals(IntegerType.BOOLEAN, inf.infer(and1));
    Map<Expression, DataType> map = inf.getTypeMap();
    assertEquals(IntegerType.BIGINT, map.get(const1));
    assertEquals(IntegerType.BIGINT, map.get(const2));
    assertEquals(RealType.FLOAT, map.get(const3));
    assertEquals(RealType.FLOAT, map.get(const4));
    assertEquals(IntegerType.BOOLEAN, map.get(comp1));
    assertEquals(IntegerType.BOOLEAN, map.get(comp2));
    assertEquals(IntegerType.BOOLEAN, map.get(and1));
    assertEquals(IntegerType.BIGINT, inf.infer(comp3)); // for now, we unify type from left to right
    assertEquals(IntegerType.BOOLEAN, inf.infer(or1));
  }
}