package com.pingcap.tikv.expression.visitor;

import static org.junit.Assert.*;

import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.expression.Not;
import com.pingcap.tikv.expression.Year;
import org.joda.time.DateTime;
import org.junit.Test;

public class PartAndFilterExprRewriterTest {

  @Test
  public void TestRewrite() {
    Expression col = ColumnRef.create("y");
    Expression col2 = ColumnRef.create("a");
    Expression partExpr = new Year(col);
    DateTime date = DateTime.parse("1995-10-10");
    // rewrite right hand side's constant. Apply year to it.
    Expression exprToBeRewrited =
        LogicalBinaryExpression.or(
            ComparisonBinaryExpression.equal(col, Constant.create(date)),
            ComparisonBinaryExpression.greaterEqual(col2, Constant.create(5)));
    PartAndFilterExprRewriter expressionRewriter = new PartAndFilterExprRewriter(partExpr);
    Expression rewrote = expressionRewriter.rewrite(exprToBeRewrited);
    assertEquals("[[[y] EQUAL 1995] OR [[a] GREATER_EQUAL 5]]", rewrote.toString());

    // not support case
    partExpr = new Not(col);
    exprToBeRewrited = ComparisonBinaryExpression.equal(col, Constant.create("1995-10-10"));
    expressionRewriter = new PartAndFilterExprRewriter(partExpr);
    rewrote = expressionRewriter.rewrite(exprToBeRewrited);
    assertNull(rewrote);
    assertTrue(expressionRewriter.isUnsupportedPartFnFound());

    // rewrite left hand. Rewrite year(y) to y.
    Expression year = new Year(col);
    exprToBeRewrited = ComparisonBinaryExpression.lessEqual(year, Constant.create("1995"));
    rewrote = expressionRewriter.rewrite(exprToBeRewrited);
    assertEquals("[[y] LESS_EQUAL \"1995\"]", rewrote.toString());

    // simple column case. No rewriting happens.
    exprToBeRewrited = ComparisonBinaryExpression.lessEqual(col, Constant.create(1));
    expressionRewriter = new PartAndFilterExprRewriter(col);
    rewrote = expressionRewriter.rewrite(exprToBeRewrited);
    assertEquals("[[y] LESS_EQUAL 1]", rewrote.toString());
  }
}
