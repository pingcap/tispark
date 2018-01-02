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
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.equal;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.expression.ArithmeticBinaryExpression;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.RealType;
import java.util.Map;
import org.junit.Test;

public class ExpressionTypeInferrerTest {

  @Test
  public void typeVerifyTest() throws Exception {
    Constant const1 = Constant.create(1, IntegerType.INT);
    Constant const2 = Constant.create(11, IntegerType.INT);
    ComparisonBinaryExpression comp1 = equal(const1, const2);

    Constant const3 = Constant.create(1.1f, RealType.FLOAT);
    Constant const4 = Constant.create(1.111f, RealType.FLOAT);
    ComparisonBinaryExpression comp2 = lessThan(const3, const4);

    Constant const5 = Constant.create(1, IntegerType.INT);
    Constant const6 = Constant.create(1.1f, RealType.FLOAT);
    ArithmeticBinaryExpression comp3 = minus(const5, const6);

    LogicalBinaryExpression and1 = LogicalBinaryExpression.and(comp1, comp2);
    LogicalBinaryExpression or1 = LogicalBinaryExpression.or(comp1, comp3);

    ExpressionTypeInferrer inf = new ExpressionTypeInferrer();
    assertEquals(IntegerType.BOOLEAN, inf.infer(and1));
    Map<Expression, DataType> map = inf.getTypeMap();
    assertEquals(IntegerType.INT, map.get(const1));
    assertEquals(IntegerType.INT, map.get(const2));
    assertEquals(RealType.FLOAT, map.get(const3));
    assertEquals(RealType.FLOAT, map.get(const4));
    assertEquals(IntegerType.BOOLEAN, map.get(comp1));
    assertEquals(IntegerType.BOOLEAN, map.get(comp2));
    assertEquals(IntegerType.BOOLEAN, map.get(and1));

    try {
      assertEquals(IntegerType.BOOLEAN, inf.infer(comp3));
      fail();
    } catch (TiExpressionException e) {}

    try {
      assertEquals(IntegerType.BOOLEAN, inf.infer(or1));
    } catch (TiExpressionException e) {}
  }

}