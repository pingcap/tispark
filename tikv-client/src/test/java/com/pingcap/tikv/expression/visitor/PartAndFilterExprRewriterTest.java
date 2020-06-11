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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.expression.visitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.FuncCallExpr;
import com.pingcap.tikv.expression.FuncCallExpr.Type;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.expression.Not;
import com.pingcap.tikv.types.DateType;
import com.pingcap.tikv.types.IntegerType;
import org.joda.time.DateTime;
import org.junit.Test;

public class PartAndFilterExprRewriterTest {

  @Test
  public void TestRewrite() {
    Expression col = ColumnRef.create("y", DateType.DATE);
    Expression col2 = ColumnRef.create("a", IntegerType.INT);
    Expression partExpr = new FuncCallExpr(col, Type.YEAR);
    DateTime date = DateTime.parse("1995-10-10");
    // rewrite right hand side's constant. Apply year to it.
    Expression exprToBeRewrited =
        LogicalBinaryExpression.or(
            ComparisonBinaryExpression.equal(col, Constant.create(date, DateType.DATE)),
            ComparisonBinaryExpression.greaterEqual(col2, Constant.create(5, IntegerType.INT)));
    PartAndFilterExprRewriter expressionRewriter = new PartAndFilterExprRewriter(partExpr);
    Expression rewrote = expressionRewriter.rewrite(exprToBeRewrited);
    assertEquals("[[y@DATE EQUAL 1995] OR [a@LONG GREATER_EQUAL 5]]", rewrote.toString());

    // not support case
    partExpr = new Not(col);
    exprToBeRewrited = ComparisonBinaryExpression.equal(col, Constant.create("1995-10-10"));
    expressionRewriter = new PartAndFilterExprRewriter(partExpr);
    rewrote = expressionRewriter.rewrite(exprToBeRewrited);
    assertNull(rewrote);
    assertTrue(expressionRewriter.isUnsupportedPartFnFound());

    // rewrite left hand. Rewrite year(y) to y.
    Expression year = new FuncCallExpr(col, Type.YEAR);
    exprToBeRewrited =
        ComparisonBinaryExpression.lessEqual(year, Constant.create("1995", IntegerType.INT));
    rewrote = expressionRewriter.rewrite(exprToBeRewrited);
    assertEquals("[y@DATE LESS_EQUAL \"1995\"]", rewrote.toString());

    // simple column case. No rewriting happens.
    exprToBeRewrited =
        ComparisonBinaryExpression.lessEqual(col, Constant.create(1, IntegerType.INT));
    expressionRewriter = new PartAndFilterExprRewriter(col);
    rewrote = expressionRewriter.rewrite(exprToBeRewrited);
    assertEquals("[y@DATE LESS_EQUAL 1]", rewrote.toString());
  }
}
