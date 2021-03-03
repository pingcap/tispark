/*
 * Copyright 2020 PingCAP, Inc.
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

package com.pingcap.tikv.codec;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.key.IntHandle;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.MySQLType;
import com.pingcap.tikv.types.StringType;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

public class TableCodecV1Test {
  private Object[] values;
  private final TiTableInfo tblInfo = createTable();

  private static TiTableInfo createTable() {
    StringType VARCHAR255 =
        new StringType(
            new TiColumnInfo.InternalTypeHolder(
                MySQLType.TypeVarchar.getTypeCode(), 0, 255, 0, "", "", ImmutableList.of()));

    return new MetaUtils.TableBuilder()
        .name("testTable")
        .addColumn("c1", IntegerType.INT, true)
        .addColumn("c2", IntegerType.BIGINT)
        // TODO: enable when support Timestamp
        // .addColumn("c3", DateTimeType.DATETIME)
        // .addColumn("c4", TimestampType.TIMESTAMP)
        .addColumn("c5", VARCHAR255)
        .addColumn("c6", VARCHAR255)
        .setPkHandle(true)
        // .appendIndex("testIndex", ImmutableList.of("c1", "c2"), false)
        .build();
  }

  private void makeValues() {
    List<Object> values = new ArrayList<>();
    values.add(new IntHandle(1L));
    values.add(1L);
    DateTime dateTime = DateTime.parse("1995-10-10");
    // values.add(new Timestamp(dateTime.getMillis()));
    // values.add(new Timestamp(dateTime.getMillis()));
    values.add("abc");
    values.add("中");
    this.values = values.toArray();
  }

  @Before
  public void setUp() {
    makeValues();
  }

  @Test
  public void testEmptyValues() {
    byte[] bytes = TableCodecV1.encodeRow(new ArrayList<>(), new Object[] {}, false);
    assertEquals(1, bytes.length);
    assertEquals(Codec.NULL_FLAG, bytes[0]);
  }

  @Test
  public void testRowCodec() {
    // multiple test was added since encodeRow refuse its cdo
    for (int i = 0; i < 4; i++) {
      byte[] bytes = TableCodecV1.encodeRow(tblInfo.getColumns(), values, tblInfo.isPkHandle());
      // testing the correctness via decodeRow
      Row row = TableCodecV1.decodeRow(bytes, new IntHandle(1L), tblInfo);
      for (int j = 0; j < tblInfo.getColumns().size(); j++) {
        assertEquals(row.get(j, null), values[j]);
      }
    }
  }
}
