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

package com.pingcap.tikv.parser;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.expression.ArithmeticBinaryExpression;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.meta.CIStr;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiPartitionDef;
import com.pingcap.tikv.meta.TiPartitionInfo.PartitionType;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.RealType;
import com.pingcap.tikv.types.StringType;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class TiParserTest {

  @Test
  public void TestYearFunction() {
    String sql = "year(purchased)";
    TiParser parser = new TiParser();
    Expression year = parser.parseExpression(sql);
    Assert.assertEquals("year([purchased])", year.toString());
  }

  @Test
  public void TestParseOR() {
    String sql = "1.0";
    TiParser parser = new TiParser();
    Expression constant = parser.parseExpression(sql);
    Assert.assertEquals(Constant.create(1.0, RealType.REAL), constant);

    sql = "id < 1 or id >= 3";
    Expression or = parser.parseExpression(sql);
    Assert.assertEquals(or.toString(), "[[[id] LESS_THAN 1] OR [[id] GREATER_EQUAL 3]]");

    sql = "id < 1 xor id >= 3";
    Expression xor = parser.parseExpression(sql);
    Assert.assertEquals(xor.toString(), "[[[id] LESS_THAN 1] XOR [[id] GREATER_EQUAL 3]]");
  }

  @Test
  public void TestParseExpression() {
    String sql = "1.0";
    TiParser parser = new TiParser();
    Expression constant = parser.parseExpression(sql);
    Assert.assertEquals(Constant.create(1.0, RealType.REAL), constant);

    sql = "true";
    Expression ex1 = parser.parseExpression(sql);
    Assert.assertEquals("1", ex1.toString());

    sql = "true and id < 4";
    ex1 = parser.parseExpression(sql);
    Assert.assertEquals("[1 AND [[id] LESS_THAN 4]]", ex1.toString());

    sql = "true and x <= 4";
    ex1 = parser.parseExpression(sql);
    Assert.assertEquals("[1 AND [[x] LESS_EQUAL 4]]", ex1.toString());

    sql = "true and x = 4";
    ex1 = parser.parseExpression(sql);
    Assert.assertEquals("[1 AND [[x] EQUAL 4]]", ex1.toString());

    sql = "true and x > 4";
    ex1 = parser.parseExpression(sql);
    Assert.assertEquals("[1 AND [[x] GREATER_THAN 4]]", ex1.toString());

    sql = "1.4;";
    Expression cst2 = parser.parseExpression(sql);
    Assert.assertEquals(Constant.create(1.4), cst2);

    sql = "id;";
    Expression colRef = parser.parseExpression(sql);
    Assert.assertEquals(ColumnRef.create("id"), colRef);

    sql = "id+1";
    colRef = parser.parseExpression(sql);
    Assert.assertEquals(
        ArithmeticBinaryExpression.plus(
            ColumnRef.create("id"), Constant.create(1, IntegerType.INT)),
        colRef);

    sql = "id*1";
    colRef = parser.parseExpression(sql);
    Assert.assertEquals(
        ArithmeticBinaryExpression.multiply(
            ColumnRef.create("id"), Constant.create(1, IntegerType.INT)),
        colRef);

    sql = "id-1";
    colRef = parser.parseExpression(sql);
    Assert.assertEquals(
        ArithmeticBinaryExpression.minus(ColumnRef.create("id"), Constant.create(1)), colRef);

    sql = "id/1";
    colRef = parser.parseExpression(sql);
    Assert.assertEquals(
        ArithmeticBinaryExpression.divide(ColumnRef.create("id"), Constant.create(1)), colRef);

    sql = "id div 1";
    colRef = parser.parseExpression(sql);
    Assert.assertEquals(
        ArithmeticBinaryExpression.divide(ColumnRef.create("id"), Constant.create(1)), colRef);

    sql = "'abc'";
    Expression stringLiteral = parser.parseExpression(sql);
    Assert.assertEquals(Constant.create("'abc'"), stringLiteral);

    sql = "id < 1 and id >= 3";
    Expression and = parser.parseExpression(sql);
    Assert.assertEquals(and.toString(), "[[[id] LESS_THAN 1] AND [[id] GREATER_EQUAL 3]]");

    sql = "id < 1 and id >= 0";
    and = parser.parseExpression(sql);
    Assert.assertEquals(and.toString(), "[[[id] LESS_THAN 1] AND [[id] GREATER_EQUAL 0]]");

    sql = "''";
    stringLiteral = parser.parseExpression(sql);
    Assert.assertEquals(stringLiteral, Constant.create("''"));

    sql = "\"abc\"";
    stringLiteral = parser.parseExpression(sql);
    Assert.assertEquals(stringLiteral, Constant.create("abc"));
  }

  private TiTableInfo createTaleInfoWithParts() {
    List<TiPartitionDef> partDefs = new ArrayList<>();
    partDefs.add(new TiPartitionDef(1L, CIStr.newCIStr("p0"), ImmutableList.of("5"), ""));
    partDefs.add(new TiPartitionDef(2L, CIStr.newCIStr("p1"), ImmutableList.of("10"), ""));
    partDefs.add(new TiPartitionDef(3L, CIStr.newCIStr("p2"), ImmutableList.of("15"), ""));
    partDefs.add(new TiPartitionDef(4L, CIStr.newCIStr("p3"), ImmutableList.of("MAXVALUE"), ""));
    return new MetaUtils.TableBuilder()
        .name("rcx")
        .addColumn("a", IntegerType.INT, true)
        .addColumn("b", IntegerType.INT)
        .addColumn("c", StringType.CHAR)
        .addColumn("d", IntegerType.INT)
        .addPartition("a", PartitionType.RangePartition, partDefs, null)
        .build();
  }

  @Test
  public void TestParseWithTableInfo() {
    TiTableInfo tableInfo = createTaleInfoWithParts();
    TiParser parser = new TiParser(tableInfo);
    Expression expr = parser.parseExpression("`a` < 5");
    Assert.assertEquals(expr.toString(), "[a@LONG LESS_THAN 5]");
  }
}
