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

import static com.pingcap.tikv.expression.ComparisonBinaryExpression.*;
import static com.pingcap.tikv.expression.LogicalBinaryExpression.*;
import static com.pingcap.tikv.predicates.PredicateUtils.expressionToIndexRanges;
import static java.util.Objects.requireNonNull;
import static org.junit.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.key.RowKey;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.meta.*;
import com.pingcap.tikv.meta.TiColumnInfo.InternalTypeHolder;
import com.pingcap.tikv.types.*;
import java.util.*;
import java.util.function.BiConsumer;
import org.junit.Test;
import org.tikv.kvproto.Coprocessor;

public class ScanAnalyzerTest {
  private static TiTableInfo createTable() {
    return createTableWithIndex(1, 1);
  }

  private static TiTableInfo createTableWithIndex(long tableId, long indexId) {
    return new MetaUtils.TableBuilder()
        .name("testTable")
        .addColumn("c1", IntegerType.INT, true)
        .addColumn("c2", StringType.VARCHAR)
        .addColumn("c3", StringType.VARCHAR)
        .addColumn("c4", IntegerType.TINYINT)
        .tableId(tableId)
        .appendIndex(indexId, "testIndex", ImmutableList.of("c1", "c2", "c3"), false)
        .build();
  }

  private static TiTableInfo createTableWithPrefix() {
    InternalTypeHolder holder =
        new InternalTypeHolder(
            MySQLType.TypeVarchar.getTypeCode(),
            0,
            3, // indicating a prefix type
            0,
            "",
            "",
            ImmutableList.of());

    DataType typePrefix = DataTypeFactory.of(holder);
    return new MetaUtils.TableBuilder()
        .name("testTable")
        .addColumn("c1", IntegerType.INT, true)
        .addColumn("c2", typePrefix)
        .addColumn("c3", StringType.VARCHAR)
        .addColumn("c4", IntegerType.INT)
        .appendIndex("testIndex", ImmutableList.of("c1", "c2", "c3"), false)
        .setPkHandle(true)
        .build();
  }

  @Test
  public void buildTableScanKeyRangeTest() {
    // This test also covers partitioned table. When it comes to partitioned table
    // we need to build key range from table ids(collect from partition definitions)
    TiTableInfo table = createTableWithIndex(6, 5);
    TiIndexInfo pkIndex = TiIndexInfo.generateFakePrimaryKeyIndex(table);

    Expression eq1 = lessThan(ColumnRef.create("c1", table), Constant.create(3, IntegerType.INT));

    List<Expression> exprs = ImmutableList.of(eq1);

    ScanSpec result = ScanAnalyzer.extractConditions(exprs, table, pkIndex);
    List<IndexRange> irs =
        expressionToIndexRanges(
            result.getPointPredicates(), result.getRangePredicate(), table, pkIndex);

    ScanAnalyzer scanAnalyzer = new ScanAnalyzer();

    List<Coprocessor.KeyRange> keyRanges = scanAnalyzer.buildTableScanKeyRange(table, irs, null);

    assertEquals(keyRanges.size(), 1);

    Coprocessor.KeyRange keyRange = keyRanges.get(0);

    assertEquals(
        ByteString.copyFrom(
            new byte[] {116, -128, 0, 0, 0, 0, 0, 0, 6, 95, 114, 0, 0, 0, 0, 0, 0, 0, 0}),
        keyRange.getStart());
    assertEquals(
        ByteString.copyFrom(
            new byte[] {116, -128, 0, 0, 0, 0, 0, 0, 6, 95, 115, 0, 0, 0, 0, 0, 0, 0, 0}),
        keyRange.getEnd());
  }

  @Test
  public void buildIndexScanKeyRangeTest() {
    TiTableInfo table = createTableWithIndex(6, 5);
    TiIndexInfo index = table.getIndices().get(0);

    Expression eq1 = equal(ColumnRef.create("c1", table), Constant.create(0));
    Expression eq2 = lessEqual(ColumnRef.create("c2", table), Constant.create("wtf"));

    List<Expression> exprs = ImmutableList.of(eq1);

    ScanSpec result = ScanAnalyzer.extractConditions(exprs, table, index);
    List<IndexRange> irs =
        expressionToIndexRanges(
            result.getPointPredicates(), result.getRangePredicate(), table, index);

    ScanAnalyzer scanAnalyzer = new ScanAnalyzer();

    List<Coprocessor.KeyRange> keyRanges =
        scanAnalyzer.buildIndexScanKeyRange(table, index, irs, null);

    assertEquals(keyRanges.size(), 1);

    Coprocessor.KeyRange keyRange = keyRanges.get(0);

    assertEquals(
        ByteString.copyFrom(
            new byte[] {
              116, -128, 0, 0, 0, 0, 0, 0, 6, 95, 105, -128, 0, 0, 0, 0, 0, 0, 5, 3, -128, 0, 0, 0,
              0, 0, 0, 0
            }),
        keyRange.getStart());
    assertEquals(
        ByteString.copyFrom(
            new byte[] {
              116, -128, 0, 0, 0, 0, 0, 0, 6, 95, 105, -128, 0, 0, 0, 0, 0, 0, 5, 3, -128, 0, 0, 0,
              0, 0, 0, 1
            }),
        keyRange.getEnd());

    exprs = ImmutableList.of(eq1, eq2);
    result = ScanAnalyzer.extractConditions(exprs, table, index);

    irs =
        expressionToIndexRanges(
            result.getPointPredicates(), result.getRangePredicate(), table, index);

    keyRanges = scanAnalyzer.buildIndexScanKeyRange(table, index, irs, null);

    assertEquals(keyRanges.size(), 1);

    keyRange = keyRanges.get(0);

    assertEquals(
        ByteString.copyFrom(
            new byte[] {
              116, -128, 0, 0, 0, 0, 0, 0, 6, 95, 105, -128, 0, 0, 0, 0, 0, 0, 5, 3, -128, 0, 0, 0,
              0, 0, 0, 0, 0
            }),
        keyRange.getStart());

    assertEquals(
        ByteString.copyFrom(
            new byte[] {
              116, -128, 0, 0, 0, 0, 0, 0, 6, 95, 105, -128, 0, 0, 0, 0, 0, 0, 5, 3, -128, 0, 0, 0,
              0, 0, 0, 0, 1, 119, 116, 102, 0, 0, 0, 0, 0, -6, 0
            }),
        keyRange.getEnd());
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

  @Test
  public void logicalBinaryExpressionToIndexRangesTest() {
    TiTableInfo table = createTableWithIndex(6, 5);
    TiIndexInfo index = table.getIndices().get(0);

    TypedKey zero = TypedKey.toTypedKey(0, IntegerType.INT);
    TypedKey one = TypedKey.toTypedKey(1, IntegerType.INT);
    TypedKey two = TypedKey.toTypedKey(2, IntegerType.INT);

    Expression eq1 = greaterThan(ColumnRef.create("c1", table), Constant.create(0));
    Expression eq2 = lessThan(ColumnRef.create("c1", table), Constant.create(2));
    Expression eq3 = lessEqual(ColumnRef.create("c1", table), Constant.create(1));
    Expression eq4 = greaterThan(ColumnRef.create("c1", table), Constant.create(2));
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
          ScanSpec result = ScanAnalyzer.extractConditions(exprs, table, index);
          List<IndexRange> irs =
              expressionToIndexRanges(
                  result.getPointPredicates(), result.getRangePredicate(), table, index);
          assertEquals(irs.size(), v.size());
          for (int i = 0; i < irs.size(); i++) {
            assertEquals(irs.get(i).getRange(), v.get(i));
          }
        });
  }

  @Test
  public void extractConditionsTest() {
    TiTableInfo table = createTable();
    TiIndexInfo index = table.getIndices().get(0);

    Expression eq1 = equal(ColumnRef.create("c1", table), Constant.create(0, IntegerType.INT));
    Expression eq2 =
        equal(ColumnRef.create("c2", table), Constant.create("test", StringType.VARCHAR));
    Expression le1 =
        lessEqual(ColumnRef.create("c3", table), Constant.create("fxxx", StringType.VARCHAR));
    // Last one should be pushed back
    Expression eq3 =
        equal(ColumnRef.create("c4", table), Constant.create("fxxx", StringType.VARCHAR));

    List<Expression> exprs = ImmutableList.of(eq1, eq2, le1, eq3);

    ScanSpec result = ScanAnalyzer.extractConditions(exprs, table, index);
    assertEquals(1, result.getResidualPredicates().size());
    assertEquals(eq3, result.getResidualPredicates().toArray()[0]);

    assertEquals(2, result.getPointPredicates().size());
    assertEquals(eq1, result.getPointPredicates().get(0));
    assertEquals(eq2, result.getPointPredicates().get(1));

    assertTrue(result.getRangePredicate().isPresent());
    assertEquals(le1, result.getRangePredicate().get());
  }

  @Test
  public void extractConditionsWithPrefixTest() {
    TiTableInfo table = createTableWithPrefix();
    TiIndexInfo index = table.getIndices().get(0);

    Expression eq1 = equal(ColumnRef.create("c1", table), Constant.create(0, IntegerType.INT));
    Expression eq2 =
        equal(ColumnRef.create("c2", table), Constant.create("test", StringType.VARCHAR));
    Expression le1 =
        lessEqual(ColumnRef.create("c3", table), Constant.create("fxxx", StringType.VARCHAR));
    // Last one should be pushed back
    Expression eq3 =
        equal(ColumnRef.create("c4", table), Constant.create("fxxx", StringType.VARCHAR));

    List<Expression> exprs = ImmutableList.of(eq1, eq2, le1, eq3);
    Set<Expression> baselineSet = ImmutableSet.of(eq2, le1, eq3);

    ScanSpec result = ScanAnalyzer.extractConditions(exprs, table, index);
    // 3 remains since c2 condition pushed back as well
    assertEquals(baselineSet, result.getResidualPredicates());

    assertEquals(2, result.getPointPredicates().size());
    assertEquals(eq1, result.getPointPredicates().get(0));
    assertEquals(eq2, result.getPointPredicates().get(1));

    assertFalse(result.getRangePredicate().isPresent());
  }

  @Test
  public void extractConditionsWithPrimaryKeyTest() {
    TiTableInfo table = createTableWithPrefix();
    TiIndexInfo index = TiIndexInfo.generateFakePrimaryKeyIndex(table);
    requireNonNull(index);
    assertEquals(1, index.getIndexColumns().size());
    assertEquals("c1", index.getIndexColumns().get(0).getName());

    Expression eq1 = equal(ColumnRef.create("c1", table), Constant.create(0, IntegerType.INT));
    Expression eq2 =
        equal(ColumnRef.create("c2", table), Constant.create("test", StringType.VARCHAR));
    Expression le1 =
        lessEqual(ColumnRef.create("c3", table), Constant.create("fxxx", StringType.VARCHAR));
    // Last one should be pushed back
    Expression eq3 =
        equal(ColumnRef.create("c4", table), Constant.create("fxxx", StringType.VARCHAR));

    List<Expression> exprs = ImmutableList.of(eq1, eq2, le1, eq3);

    ScanSpec result = ScanAnalyzer.extractConditions(exprs, table, index);

    Set<Expression> baselineSet = ImmutableSet.of(eq2, le1, eq3);
    // 3 remains since c2 condition pushed back as well
    assertEquals(baselineSet, result.getResidualPredicates());

    assertEquals(1, result.getPointPredicates().size());
    assertEquals(eq1, result.getPointPredicates().get(0));

    assertFalse(result.getRangePredicate().isPresent());
  }

  @Test
  public void testKeyRangeGenWithNoFilterTest() {
    TiTableInfo table = createTableWithPrefix();
    TiIndexInfo index = TiIndexInfo.generateFakePrimaryKeyIndex(table);
    ScanAnalyzer scanBuilder = new ScanAnalyzer();
    ScanAnalyzer.ScanPlan scanPlan =
        scanBuilder.buildIndexScan(ImmutableList.of(), ImmutableList.of(), index, table, null);

    ByteString startKey = RowKey.toRowKey(table.getId(), Long.MIN_VALUE).toByteString();
    ByteString endKey = RowKey.createBeyondMax(table.getId()).toByteString();

    assertEquals(1, scanPlan.getKeyRanges().size());
    assertEquals(startKey, scanPlan.getKeyRanges().get(0).getStart());
    assertEquals(endKey, scanPlan.getKeyRanges().get(0).getEnd());
  }

  @Test
  public void TestCoveringIndex() {
    InternalTypeHolder holder =
        new InternalTypeHolder(
            MySQLType.TypeVarchar.getTypeCode(),
            0,
            3, // indicating a prefix type
            0,
            "",
            "",
            ImmutableList.of());

    Map<String, DataType> dataTypeMap = new HashMap<>();
    dataTypeMap.put("id", IntegerType.INT);
    dataTypeMap.put("a", IntegerType.INT);
    dataTypeMap.put("b", IntegerType.INT);
    dataTypeMap.put("c", IntegerType.INT);
    dataTypeMap.put("holder", DataTypeFactory.of(holder));

    Map<String, Integer> offsetMap = new HashMap<>();
    offsetMap.put("id", 0);
    offsetMap.put("a", 1);
    offsetMap.put("b", 2);
    offsetMap.put("c", 3);
    offsetMap.put("holder", 4);

    class test {
      private String[] columnNames;
      private String[] indexNames;
      private int[] indexLens;
      private boolean isCovering;

      private test(String[] col, String[] idx, int[] idxLen, boolean result) {
        columnNames = col;
        indexNames = idx;
        indexLens = idxLen;
        isCovering = result;
      }

      private String[] getColumnNames() {
        return columnNames;
      }

      private String[] getIndexNames() {
        return indexNames;
      }

      private int[] getIndexLens() {
        return indexLens;
      }
    }

    final test[] tests = {
      new test(new String[] {"a"}, new String[] {"a"}, new int[] {-1}, true),
      new test(new String[] {"a"}, new String[] {"a", "b"}, new int[] {-1, -1}, true),
      new test(new String[] {"a", "b"}, new String[] {"b", "a"}, new int[] {-1, -1}, true),
      new test(new String[] {"a", "b"}, new String[] {"b", "c"}, new int[] {-1, -1}, false),
      new test(
          new String[] {"holder", "b"}, new String[] {"holder", "b"}, new int[] {50, -1}, false),
      new test(new String[] {"a", "b"}, new String[] {"a", "c"}, new int[] {-1, -1}, false),
      new test(new String[] {"id", "a"}, new String[] {"a", "b"}, new int[] {-1, -1}, true)
    };

    ScanAnalyzer scanBuilder = new ScanAnalyzer();

    for (test t : tests) {
      List<TiColumnInfo> columns = new ArrayList<>();
      List<TiIndexColumn> indexCols = new ArrayList<>();
      boolean pkIsHandle = false;
      for (int i = 0; i < t.getColumnNames().length; i++) {
        String colName = t.getColumnNames()[i];
        if (colName.equals("id")) {
          pkIsHandle = true;
        }
        columns.add(
            new TiColumnInfo(
                offsetMap.get(colName),
                colName,
                i,
                dataTypeMap.get(colName),
                colName.equals("id")));
      }
      for (int i = 0; i < t.getIndexNames().length; i++) {
        String idxName = t.getIndexNames()[i];
        int idxLen = t.getIndexLens()[i];
        indexCols.add(new TiIndexColumn(CIStr.newCIStr(idxName), offsetMap.get(idxName), idxLen));
      }
      TiIndexInfo indexInfo =
          new TiIndexInfo(
              1,
              CIStr.newCIStr("test_idx"),
              CIStr.newCIStr("testTable"),
              ImmutableList.copyOf(indexCols),
              false,
              false,
              SchemaState.StatePublic.getStateCode(),
              "Test Index",
              IndexType.IndexTypeBtree.getTypeCode(),
              false);
      boolean isCovering = scanBuilder.isCoveringIndex(columns, indexInfo, pkIsHandle);
      assertEquals(isCovering, t.isCovering);
    }
  }
}
