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
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.equal;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.greaterThan;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.lessEqual;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.lessThan;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.notEqual;
import static com.pingcap.tikv.expression.LogicalBinaryExpression.and;
import static com.pingcap.tikv.expression.LogicalBinaryExpression.or;
import static com.pingcap.tikv.expression.LogicalBinaryExpression.xor;
import static com.pingcap.tikv.predicates.TiKVScanAnalyzer.extractConditions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.key.CompoundKey;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.StringType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
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
  public void mergeCNFExpressionsTest() {
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

  @Test
  public void logicalBinaryExpressionToIndexRangesTest() {
    TiTableInfo table = createTable();
    TiIndexInfo index = table.getIndices().get(0);

    TypedKey zero = TypedKey.toTypedKey(0, IntegerType.INT);
    TypedKey one = TypedKey.toTypedKey(1, IntegerType.INT);
    TypedKey two = TypedKey.toTypedKey(2, IntegerType.INT);

    ColumnRef col1 = ColumnRef.create("c1", table);

    Expression eq1 = greaterThan(col1, Constant.create(0));
    Expression eq2 = lessThan(col1, Constant.create(2));
    Expression eq3 = lessEqual(col1, Constant.create(1));
    Expression eq4 = greaterThan(col1, Constant.create(2));
    Expression and1 = and(eq1, eq2);
    Expression and2 = and(eq1, eq3);
    Expression and3 = and(eq2, eq3);
    Expression or1 = or(eq1, eq2);
    Expression or2 = or(eq1, eq4);
    Expression or3 = or(eq3, eq4);
    Expression xor1 = xor(eq1, eq2);
    Expression xor2 = xor(eq1, eq3);
    Expression xor3 = xor(eq2, eq3);

    List<Range> ans1 = ImmutableList.of(Range.open(zero, two));
    List<Range> ans2 = ImmutableList.of(Range.openClosed(zero, one));
    List<Range> ans3 = ImmutableList.of(Range.atMost(one));
    List<Range> ans4 = ImmutableList.of(Range.all());
    List<Range> ans5 = ImmutableList.of(Range.greaterThan(zero));
    List<Range> ans6 = ImmutableList.of(Range.atMost(one), Range.greaterThan(two));
    List<Range> ans7 = ImmutableList.of(Range.atMost(zero), Range.atLeast(two));
    List<Range> ans8 = ImmutableList.of(Range.atMost(zero), Range.greaterThan(one));
    List<Range> ans9 = ImmutableList.of(Range.open(one, two));

    Tests<Expression, List<Range>> logicalTests = new Tests<>();
    logicalTests.addTestCase(and1, ans1);
    logicalTests.addTestCase(and2, ans2);
    logicalTests.addTestCase(and3, ans3);
    logicalTests.addTestCase(or1, ans4);
    logicalTests.addTestCase(or2, ans5);
    logicalTests.addTestCase(or3, ans6);
    logicalTests.addTestCase(xor1, ans7);
    logicalTests.addTestCase(xor2, ans8);
    logicalTests.addTestCase(xor3, ans9);

    logicalTests.test(
        (k, v) -> {
          List<Expression> exprs = ImmutableList.of(k);
          ScanSpec result = extractConditions(exprs, table, index);
          List<IndexRange> irs =
              PredicateUtils.expressionToIndexRanges(
                  result.getPointPredicates(), result.getRangePredicate(), table, index);
          assertEquals(irs.size(), v.size());
          for (int i = 0; i < irs.size(); i++) {
            assertEquals(irs.get(i).getRange(), v.get(i));
          }
        });
  }

  private class Tests<K, V> {
    List<K> input = new ArrayList<>();
    List<V> output = new ArrayList<>();

    void addTestCase(K in, V out) {
      input.add(in);
      output.add(out);
    }

    void test(BiConsumer<K, V> c) {
      for (int i = 0; i < input.size(); i++) {
        c.accept(input.get(i), output.get(i));
      }
    }
  }
}
