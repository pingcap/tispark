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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.expression.scalar.LessEqual;
import com.pingcap.tikv.key.RowKey;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiColumnInfo.InternalTypeHolder;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.StringType;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.MySQLType;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class ScanBuilderTest {
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

  private static TiTableInfo createTableWithPrefix() {
    InternalTypeHolder holder =
        new InternalTypeHolder(
            MySQLType.TypeVarchar.getTypeCode(),
            0,
            3, // indicating a prefix type
            0,
            "",
            "",
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
  public void extractConditions() throws Exception {
    TiTableInfo table = createTable();
    TiIndexInfo index = table.getIndices().get(0);

    Expression eq1 = new Equal(ColumnRef.create("c1", table), Constant.create(0));
    Expression eq2 = new Equal(ColumnRef.create("c2", table), Constant.create("test"));
    Expression le1 = new LessEqual(ColumnRef.create("c3", table), Constant.create("fxxx"));
    // Last one should be pushed back
    Expression eq3 = new Equal(ColumnRef.create("c4", table), Constant.create("fxxx"));

    List<Expression> exprs = ImmutableList.of(eq1, eq2, le1, eq3);

    ScanBuilder.IndexMatchingResult result = ScanBuilder.extractConditions(exprs, table, index);
    assertEquals(1, result.residualConditions.size());
    assertEquals(eq3, result.residualConditions.get(0));

    assertEquals(2, result.accessPoints.size());
    assertEquals(eq1, result.accessPoints.get(0));
    assertEquals(eq2, result.accessPoints.get(1));

    assertEquals(1, result.accessConditions.size());
    assertEquals(le1, result.accessConditions.get(0));
  }

  @Test
  public void extractConditionsWithPrefix() throws Exception {
    TiTableInfo table = createTableWithPrefix();
    TiIndexInfo index = table.getIndices().get(0);

    Expression eq1 = new Equal(ColumnRef.create("c1", table), Constant.create(0));
    Expression eq2 = new Equal(ColumnRef.create("c2", table), Constant.create("test"));
    Expression le1 = new LessEqual(ColumnRef.create("c3", table), Constant.create("fxxx"));
    // Last one should be pushed back
    Expression eq3 = new Equal(ColumnRef.create("c4", table), Constant.create("fxxx"));

    List<Expression> exprs = ImmutableList.of(eq1, eq2, le1, eq3);

    ScanBuilder.IndexMatchingResult result = ScanBuilder.extractConditions(exprs, table, index);
    // 3 remains since c2 condition pushed back as well
    assertEquals(3, result.residualConditions.size());
    assertEquals(eq2, result.residualConditions.get(0));
    assertEquals(le1, result.residualConditions.get(1));
    assertEquals(eq3, result.residualConditions.get(2));

    assertEquals(2, result.accessPoints.size());
    assertEquals(eq1, result.accessPoints.get(0));
    assertEquals(eq2, result.accessPoints.get(1));

    assertEquals(0, result.accessConditions.size());
  }

  @Test
  public void extractConditionsWithPrimaryKey() throws Exception {
    TiTableInfo table = createTableWithPrefix();
    TiIndexInfo index = TiIndexInfo.generateFakePrimaryKeyIndex(table);
    assertEquals(1, index.getIndexColumns().size());
    assertEquals("c1", index.getIndexColumns().get(0).getName());

    Expression eq1 = new Equal(ColumnRef.create("c1", table), Constant.create(0));
    Expression eq2 = new Equal(ColumnRef.create("c2", table), Constant.create("test"));
    Expression le1 = new LessEqual(ColumnRef.create("c3", table), Constant.create("fxxx"));
    // Last one should be pushed back
    Expression eq3 = new Equal(ColumnRef.create("c4", table), Constant.create("fxxx"));

    List<Expression> exprs = ImmutableList.of(eq1, eq2, le1, eq3);

    ScanBuilder.IndexMatchingResult result = ScanBuilder.extractConditions(exprs, table, index);

    // 3 remains since c2 condition pushed back as well
    assertEquals(3, result.residualConditions.size());
    assertEquals(eq2, result.residualConditions.get(0));
    assertEquals(le1, result.residualConditions.get(1));
    assertEquals(eq3, result.residualConditions.get(2));

    assertEquals(1, result.accessPoints.size());
    assertEquals(eq1, result.accessPoints.get(0));

    assertEquals(0, result.accessConditions.size());
  }

  @Test
  public void testKeyRangeGenWithNoFilter() throws Exception {
    TiTableInfo table = createTableWithPrefix();
    TiIndexInfo index = TiIndexInfo.generateFakePrimaryKeyIndex(table);
    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(new ArrayList<>(), index, table);

    ByteString startKey = RowKey.toRowKey(table.getId(), Long.MIN_VALUE).toByteString();
    ByteString endKey = RowKey.toRowKey(table.getId(), Long.MAX_VALUE).toByteString();

    assertEquals(1, scanPlan.getKeyRanges().size());
    assertEquals(startKey, scanPlan.getKeyRanges().get(0).getStart());
    assertEquals(endKey, scanPlan.getKeyRanges().get(0).getEnd());
  }
}
