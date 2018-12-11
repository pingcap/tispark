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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.tikv.expression.ArithmeticBinaryExpression.*;
import static org.tikv.expression.ComparisonBinaryExpression.equal;
import static org.tikv.expression.ComparisonBinaryExpression.notEqual;
import static org.tikv.expression.LogicalBinaryExpression.and;
import static org.tikv.expression.LogicalBinaryExpression.or;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;
import org.tikv.expression.ColumnRef;
import org.tikv.expression.Constant;
import org.tikv.expression.Expression;
import org.tikv.key.CompoundKey;
import org.tikv.key.Key;
import org.tikv.key.TypedKey;
import org.tikv.meta.MetaUtils;
import org.tikv.meta.TiTableInfo;
import org.tikv.types.IntegerType;
import org.tikv.types.StringType;
import shade.com.google.common.collect.ImmutableList;
import shade.com.google.common.collect.ImmutableSet;
import shade.com.google.common.collect.Range;

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

    Expression expression =
        and(
            c1,
            and(c2, and(col1, and(divide(col4, and(plus(col1, c1), minus(col2, col5))), col3))));
    Set<ColumnRef> columns = PredicateUtils.extractColumnRefFromExpression(expression);
    assertEquals(baseline, columns);
  }

  @Test
  public void expressionToIndexRangesTest() {
    TiTableInfo table = createTable();
    ColumnRef col1 = ColumnRef.create("c1", table);
    ColumnRef col4 = ColumnRef.create("c4", table);
    ColumnRef col5 = ColumnRef.create("c5", table);
    Constant c1 = Constant.create(1, IntegerType.INT);
    Constant c2 = Constant.create(2, IntegerType.INT);
    Constant c3 = Constant.create(3, IntegerType.INT);
    Constant c4 = Constant.create(4, IntegerType.INT);
    TypedKey key1 = TypedKey.toTypedKey(1, IntegerType.INT);
    TypedKey key2 = TypedKey.toTypedKey(2, IntegerType.INT);
    TypedKey key3 = TypedKey.toTypedKey(3, IntegerType.INT);
    TypedKey key4 = TypedKey.toTypedKey(4, IntegerType.INT);

    Expression predicate1 = or(or(equal(c1, col1), equal(col1, c2)), equal(col1, c1));
    Expression predicate2 = or(equal(c3, col4), equal(c4, col4));
    Expression rangePredicate = notEqual(col5, c1);
    List<IndexRange> indexRanges =
        PredicateUtils.expressionToIndexRanges(
            ImmutableList.of(predicate1, predicate2), Optional.of(rangePredicate), table, null);
    assertEquals(8, indexRanges.size());
    Key indexKey1 = CompoundKey.concat(key1, key3);
    Key indexKey2 = CompoundKey.concat(key1, key4);
    Key indexKey3 = CompoundKey.concat(key2, key3);
    Key indexKey4 = CompoundKey.concat(key2, key4);

    Range<TypedKey> baselineRange1 = Range.lessThan(key1);
    Range<TypedKey> baselineRange2 = Range.greaterThan(key1);

    Set<Key> baselineKeys = ImmutableSet.of(indexKey1, indexKey2, indexKey3, indexKey4);
    Set<Range<TypedKey>> baselineRanges = ImmutableSet.of(baselineRange1, baselineRange2);
    for (IndexRange range : indexRanges) {
      assertTrue(baselineKeys.contains(range.getAccessKey()));
      assertTrue(baselineRanges.contains(range.getRange()));
    }
  }
}
