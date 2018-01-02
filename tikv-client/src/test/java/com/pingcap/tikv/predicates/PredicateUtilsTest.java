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

package com.pingcap.tikv.predicates;

import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.divide;
import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.minus;
import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.plus;
import static com.pingcap.tikv.expression.LogicalBinaryExpression.and;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.StringType;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class PredicateUtilsTest {
  private static TiTableInfo createTable() {
    return new MetaUtils.TableBuilder()
        .name("testTable")
        .addColumn("c1", IntegerType.INT, true)
        .addColumn("c2", StringType.VARCHAR)
        .addColumn("c3", StringType.VARCHAR)
        .addColumn("c4", IntegerType.INT)
        .addColumn("c5", IntegerType.INT)
        .appendIndex("testIndex", ImmutableList.of("c1", "c2"), false)
        .build();
  }

  @Test
  public void mergeCNFExpressionsTest() throws Exception {
    Constant c1 = Constant.create(1, IntegerType.INT);
    Constant c2 = Constant.create(2, IntegerType.INT);
    Constant c3 = Constant.create(3, IntegerType.INT);
    Constant c4 = Constant.create(4, IntegerType.INT);
    Constant c5 = Constant.create(5, IntegerType.INT);
    List<Expression> exprs = ImmutableList.of(c1, c2, c3, c4, c5);

    Expression res = and(c1, and(c2, and(c3, and(c4, c5))));
    assertEquals(res, PredicateUtils.mergeCNFExpressions(exprs));
  }

  @Test
  public void extractColumnRefFromExpressionTest() {
    TiTableInfo table = createTable();
    Constant c1 = Constant.create(1, IntegerType.INT);
    Constant c2 = Constant.create(2, IntegerType.INT);
    ColumnRef col1 = ColumnRef.create("c1", table);
    ColumnRef col2 = ColumnRef.create("c2", table);
    ColumnRef col3 = ColumnRef.create("c3", table);
    ColumnRef col4 = ColumnRef.create("c4", table);
    ColumnRef col5 = ColumnRef.create("c5", table);
    Set<ColumnRef> baseline = ImmutableSet.of(col1, col2, col3, col4, col5);

    Expression expression = and(c1, and(c2, and(col1, and(divide(col4, and(plus(col1, c1), minus(col2, col5))), col3))));
    Set<ColumnRef> columns = PredicateUtils.extractColumnRefFromExpression(expression);
    assertEquals(baseline, columns);
  }
}
