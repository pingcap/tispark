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

import static org.junit.Assert.assertArrayEquals;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.key.CommonHandle;
import com.pingcap.tikv.key.Handle;
import com.pingcap.tikv.key.IntHandle;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.StringType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TableCodecTest {
  @Rule public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testRowCodecThrowException() {
    try {
      TiColumnInfo col1 = new TiColumnInfo(1, "pk", 0, IntegerType.BIGINT, true);
      TiColumnInfo col2 = new TiColumnInfo(2, "c1", 1, IntegerType.BIGINT, false);
      TiColumnInfo colIgnored = new TiColumnInfo(-1, "cIgnored", -1, IntegerType.BIGINT, false);
      TableCodec.encodeRow(
          ImmutableList.of(col1, col2, colIgnored, colIgnored), new Object[] {1L, 2L}, true, false);
      expectedEx.expect(IllegalAccessException.class);
      expectedEx.expectMessage("encodeRow error: data and columnID count not match 4 vs 2");
    } catch (IllegalAccessException ignored) {
    }
  }

  @Test
  public void testIndexValueCodec() {
    Handle commonHandle =
        CommonHandle.newCommonHandle(new DataType[] {StringType.VARCHAR}, new Object[] {"1"});
    // test common handle version0
    byte[] version0Value = TableCodec.genIndexValue(commonHandle, 0, true);
    Handle decodeCommonHandle0 = TableCodec.decodeHandleInUniqueIndexValue(version0Value, true);
    assertArrayEquals(commonHandle.encoded(), decodeCommonHandle0.encoded());

    // test common handle version1
    byte[] version1Value = TableCodec.genIndexValue(commonHandle, 1, true);
    Handle decodeCommonHandle1 = TableCodec.decodeHandleInUniqueIndexValue(version1Value, true);
    assertArrayEquals(commonHandle.encoded(), decodeCommonHandle1.encoded());

    // test int handle
    Handle intHandle = new IntHandle(1);
    byte[] intHandleValue = TableCodec.genIndexValue(intHandle, 0, true);
    Handle decodeIntHandle = TableCodec.decodeHandleInUniqueIndexValue(intHandleValue, false);
    assertArrayEquals(intHandle.encoded(), decodeIntHandle.encoded());
  }
}
