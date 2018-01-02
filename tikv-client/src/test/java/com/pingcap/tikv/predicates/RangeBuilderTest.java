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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.expression.scalar.GreaterEqual;
import com.pingcap.tikv.expression.scalar.GreaterThan;
import com.pingcap.tikv.expression.scalar.In;
import com.pingcap.tikv.expression.scalar.NotEqual;
import com.pingcap.tikv.key.CompoundKey;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.StringType;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

public class RangeBuilderTest {
  private static TiTableInfo createTable() {
    return new MetaUtils.TableBuilder()
        .name("testTable")
        .addColumn("c1", IntegerType.INT, true)
        .addColumn("c2", StringType.VARCHAR)
        .addColumn("c3", StringType.VARCHAR)
        .addColumn("c4", IntegerType.INT)
        .appendIndex("testIndex", ImmutableList.of("c1", "c2", "c3"), false)
        .build();
  }

  private static boolean testPointIndexRanges(List<Key> keys, List<List<Object>> values) {
    if (keys.size() != values.size()) return false;

    for (Key key : keys) {
      CompoundKey compKey = (CompoundKey)key;
      List<Key> pointKeys = compKey.getKeys();
      List<Object> pointObject = pointKeys
          .stream()
          .map(k -> ((TypedKey)k).getValue()).collect(Collectors.toList());
      int pos = values.indexOf(pointObject);
      if (pos == -1) {
        return false;
      }
      values.remove(pos);
    }

    return values.isEmpty();
  }


  @Test
  public void expressionToPointsTest() throws Exception {
    TiTableInfo table = createTable();
    List<TiExpr> conds =
        ImmutableList.of(
            new Equal(TiColumnRef.create("c1", table), TiConstant.create(0)),
            new Equal(TiConstant.create("v1"), TiColumnRef.create("c2", table))
        );
    List<DataType> types = ImmutableList.of(IntegerType.INT, StringType.VARCHAR);
    RangeBuilder builder = new RangeBuilder();
    List<Key> keys = builder.expressionToPoints(conds, types);
    assertEquals(1, keys.size());
    CompoundKey compoundKey = (CompoundKey)keys.get(0);
    assertEquals(2, compoundKey.getKeys().size());
    TypedKey intKey = (TypedKey) compoundKey.getKeys().get(0);
    assertEquals(0L, intKey.getValue());
    TypedKey strKey = (TypedKey) compoundKey.getKeys().get(1);
    assertEquals("v1", strKey.getValue());

    // In Expr
    conds =
        ImmutableList.of(
            new In(
                TiColumnRef.create("c1", table),
                TiConstant.create(0),
                TiConstant.create(1),
                TiConstant.create(3)),
            new Equal(TiConstant.create("v1"), TiColumnRef.create("c2", table)),
            new In(
                TiColumnRef.create("c3", table), TiConstant.create("2"), TiConstant.create("4")));
    types = ImmutableList.of(IntegerType.INT, StringType.VARCHAR, StringType.VARCHAR);

    keys = builder.expressionToPoints(conds, types);
    assertEquals(6, keys.size());
    assertTrue(
        testPointIndexRanges(
            keys,
            Lists.newArrayList(
                ImmutableList.of(0L, "v1", "2"),
                ImmutableList.of(0L, "v1", "4"),
                ImmutableList.of(1L, "v1", "2"),
                ImmutableList.of(1L, "v1", "4"),
                ImmutableList.of(3L, "v1", "2"),
                ImmutableList.of(3L, "v1", "4"))));
  }


  @Test
  public void exprToRanges() throws Exception {
    TiTableInfo table = createTable();
    List<TiExpr> conds =
        ImmutableList.of(
            new GreaterEqual(TiColumnRef.create("c1", table), TiConstant.create(0L)), // c1 >= 0
            new GreaterThan(
                TiConstant.create(100L), TiColumnRef.create("c1", table)), // 100 > c1 -> c1 < 100
            new NotEqual(TiColumnRef.create("c1", table), TiConstant.create(50L)) // c1 != 50
            );
    DataType type = IntegerType.BIGINT;
    RangeBuilder builder = new RangeBuilder();
    List<Range<TypedKey>> ranges = RangeBuilder.expressionToRanges(conds, type);
    assertEquals(2, ranges.size());
    assertEquals(Range.closedOpen(TypedKey.toTypedKey(0L, IntegerType.BIGINT),
                                  TypedKey.toTypedKey(50L, IntegerType.BIGINT)), ranges.get(0));
    assertEquals(Range.open(TypedKey.toTypedKey(50L, IntegerType.BIGINT),
                            TypedKey.toTypedKey(100L, IntegerType.BIGINT)), ranges.get(1));

    // Test points and string range
    List<TiExpr> ac =
        ImmutableList.of(
            new In(TiColumnRef.create("c1", table), TiConstant.create(0L), TiConstant.create(1L)),
            new Equal(TiConstant.create("v1"), TiColumnRef.create("c2", table)));
    List<DataType> types = ImmutableList.of(IntegerType.BIGINT, StringType.VARCHAR);
    List<Key> keys = builder.expressionToPoints(ac, types);
    assertTrue(
        testPointIndexRanges(
            keys,
            Lists.newArrayList(ImmutableList.of(0L, "v1"), ImmutableList.of(1L, "v1"))));

    conds =
        ImmutableList.of(
            new GreaterEqual(TiColumnRef.create("c3", table), TiConstant.create("a")), // c3 >= a
            new GreaterThan(
                TiConstant.create("z"), TiColumnRef.create("c3", table)), // z > c3 -> c3 < z
            new NotEqual(TiColumnRef.create("c3", table), TiConstant.create("g")) // c3 != g
            );
    type = StringType.VARCHAR;
    ranges = RangeBuilder.expressionToRanges(conds, type);

    assertEquals(2, ranges.size());

    assertEquals(Range.closedOpen(TypedKey.toTypedKey("a", StringType.VARCHAR),
                                  TypedKey.toTypedKey("g", StringType.VARCHAR)), ranges.get(0));
    assertEquals(Range.open(TypedKey.toTypedKey("g", StringType.VARCHAR),
                            TypedKey.toTypedKey("z", StringType.VARCHAR)), ranges.get(1));
  }
}
