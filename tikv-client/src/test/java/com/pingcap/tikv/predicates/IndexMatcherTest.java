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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.And;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.expression.scalar.GreaterEqual;
import com.pingcap.tikv.expression.scalar.In;
import com.pingcap.tikv.expression.scalar.LessEqual;
import com.pingcap.tikv.expression.scalar.LessThan;
import com.pingcap.tikv.expression.scalar.Or;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.StringType;
import com.pingcap.tikv.types.IntegerType;
import org.junit.Test;

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

    // index col = c1, long
    TiExpr cond = new Equal(TiColumnRef.create("c1", table), TiConstant.create(1));
    assertTrue(matcher.match(cond));

    cond = new Equal(TiConstant.create(1), TiColumnRef.create("c1", table));
    assertTrue(matcher.match(cond));

    cond = new Equal(TiColumnRef.create("c2", table), TiColumnRef.create("c1", table));
    assertFalse(matcher.match(cond));

    cond = new Equal(TiConstant.create(1), TiConstant.create(1));
    assertFalse(matcher.match(cond));

    cond =
        new And(
            new Equal(TiConstant.create(1), TiColumnRef.create("c1", table)),
            new Equal(TiColumnRef.create("c1", table), TiConstant.create(2)));
    assertFalse(matcher.match(cond));

    cond =
        new Or(
            new Equal(TiConstant.create(1), TiColumnRef.create("c1", table)),
            new Equal(TiColumnRef.create("c1", table), TiConstant.create(2)));
    assertTrue(matcher.match(cond));

    cond = new In(TiColumnRef.create("c1", table), TiConstant.create(1), TiConstant.create(2));
    assertTrue(matcher.match(cond));

    cond =
        new In(
            new Equal(TiColumnRef.create("c1", table), TiConstant.create(2)),
            TiConstant.create(1),
            TiConstant.create(2));
    assertFalse(matcher.match(cond));

    cond = new LessEqual(TiConstant.create(0), TiColumnRef.create("c1", table));
    assertFalse(matcher.match(cond));
  }

  @Test
  public void matchAll() throws Exception {
    TiTableInfo table = createTable();
    TiIndexInfo index = table.getIndices().get(0);
    TiIndexColumn col = index.getIndexColumns().get(0);
    IndexMatcher matcher = IndexMatcher.matcher(col);

    // index col = c1, long
    TiExpr cond = new LessEqual(TiColumnRef.create("c1", table), TiConstant.create(1));
    assertTrue(matcher.match(cond));

    cond = new GreaterEqual(TiConstant.create(1), TiColumnRef.create("c1", table));
    assertTrue(matcher.match(cond));

    cond = new LessThan(TiColumnRef.create("c2", table), TiColumnRef.create("c1", table));
    assertFalse(matcher.match(cond));

    cond = new LessThan(TiConstant.create(1), TiConstant.create(1));
    assertFalse(matcher.match(cond));

    cond =
        new And(
            new LessThan(TiConstant.create(1), TiColumnRef.create("c1", table)),
            new LessThan(TiColumnRef.create("c1", table), TiConstant.create(2)));
    assertTrue(matcher.match(cond));

    cond =
        new Or(
            new LessThan(TiConstant.create(1), TiColumnRef.create("c1", table)),
            new LessThan(TiColumnRef.create("c1", table), TiConstant.create(2)));
    assertTrue(matcher.match(cond));

    cond = new In(TiColumnRef.create("c1", table), TiConstant.create(1), TiConstant.create(2));
    assertTrue(matcher.match(cond));

    cond =
        new In(
            new Equal(TiColumnRef.create("c1", table), TiConstant.create(2)),
            TiConstant.create(1),
            TiConstant.create(2));
    assertFalse(matcher.match(cond));
  }
}
