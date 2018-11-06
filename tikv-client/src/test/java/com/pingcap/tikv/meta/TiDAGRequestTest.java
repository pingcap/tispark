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

package com.pingcap.tikv.meta;

import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.plus;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.lessEqual;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tikv.expression.*;
import com.pingcap.tikv.expression.AggregateFunction.FunctionType;
import com.pingcap.tikv.expression.visitor.ExpressionTypeCoercer;
import com.pingcap.tikv.expression.visitor.ProtoConverter;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.StringType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class TiDAGRequestTest {
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
  public void testTopNCouldPushDownLimit0() {
    TiTableInfo table = createTable();
    TiDAGRequest dagRequest = new TiDAGRequest(TiDAGRequest.PushDownType.NORMAL);
    ColumnRef col1 = ColumnRef.create("c1", table);
    dagRequest.addOrderByItem(ByItem.create(col1, false));
    dagRequest.addRequiredColumn(col1);
    dagRequest.setLimit(0);
    dagRequest.setTableInfo(table);
    dagRequest.setStartTs(1);
    dagRequest.buildScan(false);
  }

  @Test
  public void testSerializable() throws Exception {
    TiTableInfo table = createTable();
    TiDAGRequest selReq = new TiDAGRequest(TiDAGRequest.PushDownType.NORMAL);
    Constant c1 = Constant.create(1L, IntegerType.BIGINT);
    Constant c2 = Constant.create(2L, IntegerType.BIGINT);
    ColumnRef col1 = ColumnRef.create("c1", table);
    ColumnRef col2 = ColumnRef.create("c2", table);
    ColumnRef col3 = ColumnRef.create("c3", table);

    AggregateFunction sum = AggregateFunction.newCall(FunctionType.Sum, col1);
    AggregateFunction min = AggregateFunction.newCall(FunctionType.Min, col1);

    selReq
        .addRequiredColumn(col1)
        .addRequiredColumn(col2)
        .addRequiredColumn(col3)
        .addAggregate(sum, ExpressionTypeCoercer.inferType(sum))
        .addAggregate(min, ExpressionTypeCoercer.inferType(min))
        .addFilter(plus(c1, c2))
        .addGroupByItem(ByItem.create(ColumnRef.create("c2", table), true))
        .addOrderByItem(ByItem.create(ColumnRef.create("c3", table), false))
        .setTableInfo(table)
        .setStartTs(666)
        .setTruncateMode(TiDAGRequest.TruncateMode.IgnoreTruncation)
        .setDistinct(true)
        .setIndexInfo(table.getIndices().get(0))
        .setHaving(lessEqual(col3, c2))
        .setLimit(100)
        .addRanges(
            ImmutableList.of(
                Coprocessor.KeyRange.newBuilder()
                    .setStart(ByteString.copyFromUtf8("startkey"))
                    .setEnd(ByteString.copyFromUtf8("endkey"))
                    .build()));

    ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(byteOutStream);
    oos.writeObject(selReq);

    ByteArrayInputStream byteInStream = new ByteArrayInputStream(byteOutStream.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(byteInStream);
    TiDAGRequest derselReq = (TiDAGRequest) ois.readObject();
    assertTrue(selectRequestEquals(selReq, derselReq));
  }

  private static boolean selectRequestEquals(TiDAGRequest lhs, TiDAGRequest rhs) {
    assertEquals(lhs.getFields().size(), rhs.getFields().size());
    Map<ColumnRef, Integer> lhsMap = new HashMap<>();
    Map<ColumnRef, Integer> rhsMap = new HashMap<>();
    for (int i = 0; i < lhs.getFields().size(); i++) {
      ColumnRef lCol = lhs.getFields().get(i);
      ColumnRef rCol = rhs.getFields().get(i);
      lhsMap.put(lCol, i);
      rhsMap.put(rCol, i);
    }
    for (int i = 0; i < lhs.getFields().size(); i++) {
      Expression lhsExpr = lhs.getFields().get(i);
      Expression rhsExpr = rhs.getFields().get(i);
      Expr lhsExprProto = ProtoConverter.toProto(lhsExpr, lhsMap);
      Expr rhsExprProto = ProtoConverter.toProto(rhsExpr, rhsMap);

      if (!lhsExprProto.equals(rhsExprProto)) return false;
    }

    assertEquals(lhs.getAggregates().size(), rhs.getAggregates().size());
    for (int i = 0; i < lhs.getAggregates().size(); i++) {
      Expression lhsExpr = lhs.getAggregates().get(i);
      Expression rhsExpr = rhs.getAggregates().get(i);

      Expr lhsExprProto = ProtoConverter.toProto(lhsExpr, lhsMap);
      Expr rhsExprProto = ProtoConverter.toProto(rhsExpr, rhsMap);

      if (!lhsExprProto.equals(rhsExprProto)) return false;
    }

    assertEquals(lhs.getGroupByItems().size(), rhs.getGroupByItems().size());
    for (int i = 0; i < lhs.getGroupByItems().size(); i++) {
      ByItem lhsItem = lhs.getGroupByItems().get(i);
      ByItem rhsItem = rhs.getGroupByItems().get(i);
      if (!lhsItem.toProto(lhsMap).equals(rhsItem.toProto(rhsMap))) return false;
    }

    assertEquals(lhs.getOrderByItems().size(), rhs.getOrderByItems().size());
    for (int i = 0; i < lhs.getOrderByItems().size(); i++) {
      ByItem lhsItem = lhs.getOrderByItems().get(i);
      ByItem rhsItem = rhs.getOrderByItems().get(i);
      if (!lhsItem.toProto(lhsMap).equals(rhsItem.toProto(rhsMap))) return false;
    }

    assertEquals(lhs.getRanges().size(), rhs.getRanges().size());
    for (int i = 0; i < lhs.getRanges().size(); i++) {
      Coprocessor.KeyRange lhsItem = lhs.getRanges().get(i);
      Coprocessor.KeyRange rhsItem = rhs.getRanges().get(i);
      if (!lhsItem.equals(rhsItem)) return false;
    }

    assertEquals(lhs.getFilters().size(), rhs.getFilters().size());
    for (int i = 0; i < lhs.getFilters().size(); i++) {
      Expression lhsItem = lhs.getFilters().get(i);
      Expression rhsItem = rhs.getFilters().get(i);

      Expr lhsExprProto = ProtoConverter.toProto(lhsItem);
      Expr rhsExprProto = ProtoConverter.toProto(rhsItem);

      if (!lhsExprProto.equals(rhsExprProto)) return false;
    }

    assertEquals(lhs.getTableInfo().toProto(), rhs.getTableInfo().toProto());
    assertEquals(lhs.getLimit(), rhs.getLimit());
    assertEquals(lhs.isDistinct(), rhs.isDistinct());
    assertEquals(
        lhs.getIndexInfo().toProto(lhs.getTableInfo()),
        rhs.getIndexInfo().toProto(rhs.getTableInfo()));
    assertEquals(lhs.getStartTs(), rhs.getStartTs());
    assertEquals(lhs.getTimeZoneOffset(), rhs.getTimeZoneOffset());
    assertEquals(lhs.getFlags(), rhs.getFlags());
    return true;
  }
}
