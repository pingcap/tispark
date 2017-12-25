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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.expression.TiByItem;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.aggregate.Min;
import com.pingcap.tikv.expression.aggregate.Sum;
import com.pingcap.tikv.expression.scalar.LessEqual;
import com.pingcap.tikv.expression.scalar.Plus;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.Types;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TiDAGRequestTest {
  private static TiTableInfo createTable() {
    return new MetaUtils.TableBuilder()
        .name("testTable")
        .addColumn("c1", DataTypeFactory.of(Types.TYPE_LONG), true)
        .addColumn("c2", DataTypeFactory.of(Types.TYPE_STRING))
        .addColumn("c3", DataTypeFactory.of(Types.TYPE_STRING))
        .addColumn("c4", DataTypeFactory.of(Types.TYPE_TINY))
        .appendIndex("testIndex", ImmutableList.of("c1", "c2"), false)
        .build();
  }

  @Test
  public void testSerializable() throws Exception {
    TiTableInfo table = createTable();
    TiDAGRequest selReq = new TiDAGRequest(TiDAGRequest.PushDownType.NORMAL);
    selReq
        .addRequiredColumn(TiColumnRef.create("c1", table))
        .addRequiredColumn(TiColumnRef.create("c2", table))
        .addAggregate(new Sum(TiColumnRef.create("c1", table)))
        .addAggregate(new Min(TiColumnRef.create("c1", table)))
        .addWhere(new Plus(TiConstant.create(1L), TiConstant.create(2L)))
        .addGroupByItem(
            TiByItem.create(TiColumnRef.create("c2", table), true))
        .addOrderByItem(
            TiByItem.create(TiColumnRef.create("c3", table), false))
        .setTableInfo(table)
        .setStartTs(666)
        .setTruncateMode(TiDAGRequest.TruncateMode.IgnoreTruncation)
        .setDistinct(true)
        .setIndexInfo(table.getIndices().get(0))
        .setHaving(new LessEqual(TiColumnRef.create("c3", table), TiConstant.create(2L)))
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

  public static boolean selectRequestEquals(TiDAGRequest lhs, TiDAGRequest rhs) {
    assertEquals(lhs.getFields().size(), rhs.getFields().size());
    for (int i = 0; i < lhs.getFields().size(); i++) {
      TiExpr lhsExpr = lhs.getFields().get(i);
      TiExpr rhsExpr = rhs.getFields().get(i);
      if (!lhsExpr.toProto().equals(rhsExpr.toProto())) return false;
    }

    assertEquals(lhs.getAggregates().size(), rhs.getAggregates().size());
    for (int i = 0; i < lhs.getAggregates().size(); i++) {
      TiExpr lhsExpr = lhs.getAggregates().get(i);
      TiExpr rhsExpr = rhs.getAggregates().get(i);
      if (!lhsExpr.toProto().equals(rhsExpr.toProto())) return false;
    }

    assertEquals(lhs.getGroupByItems().size(), rhs.getGroupByItems().size());
    for (int i = 0; i < lhs.getGroupByItems().size(); i++) {
      TiByItem lhsItem = lhs.getGroupByItems().get(i);
      TiByItem rhsItem = rhs.getGroupByItems().get(i);
      if (!lhsItem.toProto().equals(rhsItem.toProto())) return false;
    }

    assertEquals(lhs.getOrderByItems().size(), rhs.getOrderByItems().size());
    for (int i = 0; i < lhs.getOrderByItems().size(); i++) {
      TiByItem lhsItem = lhs.getOrderByItems().get(i);
      TiByItem rhsItem = rhs.getOrderByItems().get(i);
      if (!lhsItem.toProto().equals(rhsItem.toProto())) return false;
    }

    assertEquals(lhs.getRanges().size(), rhs.getRanges().size());
    for (int i = 0; i < lhs.getRanges().size(); i++) {
      Coprocessor.KeyRange lhsItem = lhs.getRanges().get(i);
      Coprocessor.KeyRange rhsItem = rhs.getRanges().get(i);
      if (!lhsItem.equals(rhsItem)) return false;
    }

    assertEquals(lhs.getWhere().size(), rhs.getWhere().size());
    for (int i = 0; i < lhs.getWhere().size(); i++) {
      TiExpr lhsItem = lhs.getWhere().get(i);
      TiExpr rhsItem = rhs.getWhere().get(i);
      if (!lhsItem.toProto().equals(rhsItem.toProto())) return false;
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
