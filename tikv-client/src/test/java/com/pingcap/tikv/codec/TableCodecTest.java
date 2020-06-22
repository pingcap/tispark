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

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.types.IntegerType;
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
}
