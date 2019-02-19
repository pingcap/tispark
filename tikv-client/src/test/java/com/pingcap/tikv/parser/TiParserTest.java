package com.pingcap.tikv.parser;

import com.pingcap.tikv.expression.ArithmeticBinaryExpression;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.types.RealType;
import org.junit.Assert;
import org.junit.Test;

public class TiParserTest {

  @Test
  public void TestParseExpression() {
    String sql = "1.0";
    TiParser parser = new TiParser();
    Expression constant = parser.parseExpression(sql);
    Assert.assertEquals(constant, Constant.create(1.0, RealType.REAL));

    sql = "1.4;";
    Expression cst2 = parser.parseExpression(sql);
    Assert.assertEquals(cst2, Constant.create(1.4));

    sql = "id;";
    Expression colRef = parser.parseExpression(sql);
    Assert.assertEquals(ColumnRef.create("id"), colRef);

    sql = "id+1";
    colRef = parser.parseExpression(sql);
    Assert.assertEquals(
        colRef,
        ArithmeticBinaryExpression.plus(
            ColumnRef.create("id"), Constant.create(1.0, RealType.REAL)));

    sql = "id*1";
    colRef = parser.parseExpression(sql);
    Assert.assertEquals(
        colRef,
        ArithmeticBinaryExpression.multiply(
            ColumnRef.create("id"), Constant.create(1.0, RealType.REAL)));

    sql = "id-1";
    colRef = parser.parseExpression(sql);
    Assert.assertEquals(
        colRef,
        ArithmeticBinaryExpression.minus(
            ColumnRef.create("id"), Constant.create(1.0, RealType.REAL)));

    sql = "id/1";
    colRef = parser.parseExpression(sql);
    Assert.assertEquals(
        colRef,
        ArithmeticBinaryExpression.divide(
            ColumnRef.create("id"), Constant.create(1.0, RealType.REAL)));

    sql = "id div 1";
    colRef = parser.parseExpression(sql);
    Assert.assertEquals(
        colRef,
        ArithmeticBinaryExpression.divide(
            ColumnRef.create("id"), Constant.create(1.0, RealType.REAL)));

    sql = "'abc'";
    Expression stringLiteral = parser.parseExpression(sql);
    Assert.assertEquals(stringLiteral, Constant.create("'abc'"));

    sql = "id < 1 and id >= 3";
    Expression and = parser.parseExpression(sql);
    Assert.assertEquals(and.toString(), "[[[id] LESS_THAN 1.0] AND [[id] GREATER_EQUAL 3.0]]");
  }
}
