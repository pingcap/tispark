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

package org.tikv.predicates;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.tikv.expression.ComparisonBinaryExpression.equal;
import static org.tikv.expression.ComparisonBinaryExpression.greaterEqual;
import static org.tikv.expression.ComparisonBinaryExpression.lessEqual;
import static org.tikv.expression.ComparisonBinaryExpression.lessThan;
import static org.tikv.expression.LogicalBinaryExpression.and;
import static org.tikv.expression.LogicalBinaryExpression.or;

import org.junit.Test;
import org.tikv.expression.ColumnRef;
import org.tikv.expression.Constant;
import org.tikv.expression.Expression;
import org.tikv.expression.visitor.IndexMatcher;
import org.tikv.meta.MetaUtils;
import org.tikv.meta.TiIndexColumn;
import org.tikv.meta.TiIndexInfo;
import org.tikv.meta.TiTableInfo;
import org.tikv.types.IntegerType;
import org.tikv.types.StringType;
import shade.com.google.common.collect.ImmutableList;

public class IndexMatcherTest {
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

  @Test
  public void matchOnlyEq() throws Exception {
    TiTableInfo table = createTable();
    TiIndexInfo index = table.getIndices().get(0);
    TiIndexColumn col = index.getIndexColumns().get(0);
    IndexMatcher matcher = IndexMatcher.equalOnlyMatcher(col);
    Constant c0 = Constant.create(0, IntegerType.INT);
    Constant c1 = Constant.create(1, IntegerType.INT);
    Constant c2 = Constant.create(2, IntegerType.INT);
    ColumnRef col1 = ColumnRef.create("c1", table);
    ColumnRef col2 = ColumnRef.create("c2", table);

    // index col = c1, long
    Expression cond = equal(col1, c1);
    assertTrue(matcher.match(cond));

    cond = equal(c1, col1);
    assertTrue(matcher.match(cond));

    cond = equal(col2, col1);
    assertFalse(matcher.match(cond));

    cond = equal(c1, c1);
    assertFalse(matcher.match(cond));

    cond = and(equal(c1, col1), equal(col1, c2));
    assertFalse(matcher.match(cond));

    cond = or(equal(c1, col1), equal(col1, c2));
    assertTrue(matcher.match(cond));

    cond = lessEqual(c0, col1);
    assertFalse(matcher.match(cond));
  }

  @Test
  public void matchAll() throws Exception {
    TiTableInfo table = createTable();
    TiIndexInfo index = table.getIndices().get(0);
    TiIndexColumn col = index.getIndexColumns().get(0);
    IndexMatcher matcher = IndexMatcher.matcher(col);
    Constant c0 = Constant.create(0, IntegerType.INT);
    Constant c1 = Constant.create(1, IntegerType.INT);
    Constant c2 = Constant.create(2, IntegerType.INT);
    ColumnRef col1 = ColumnRef.create("c1", table);
    ColumnRef col2 = ColumnRef.create("c2", table);

    // index col = c1, long
    Expression cond = lessEqual(col1, c1);
    assertTrue(matcher.match(cond));

    cond = greaterEqual(c1, col1);
    assertTrue(matcher.match(cond));

    cond = lessThan(ColumnRef.create("c2", table), ColumnRef.create("c1", table));
    assertFalse(matcher.match(cond));

    cond = lessThan(c1, c1);
    assertFalse(matcher.match(cond));

    cond = and(lessThan(c1, col1), lessThan(col1, c2));
    assertTrue(matcher.match(cond));

    cond = or(lessThan(c1, col1), lessThan(col1, c2));
    assertTrue(matcher.match(cond));
  }
}
