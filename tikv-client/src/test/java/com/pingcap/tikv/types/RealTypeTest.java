/*
 *
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
 *
 */

package com.pingcap.tikv.types;

import static org.junit.Assert.assertEquals;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.types.DataType.EncodeType;
import org.junit.Test;

public class RealTypeTest {
  private static byte[] encode(Object val, EncodeType encodeType, DataType type) {
    CodecDataOutput cdo = new CodecDataOutput();
    type.encode(cdo, encodeType, val);
    return cdo.toBytes();
  }

  private static Object decode(byte[] val, DataType type) {
    return type.decode(new CodecDataInput(val));
  }

  @Test
  public void encodeTest() {
    DataType type = RealType.DOUBLE;
    double originalVal = 666.66;
    byte[] encodedKey = encode(originalVal, EncodeType.KEY, type);
    Object val = decode(encodedKey, type);
    assertEquals(originalVal, (double) val, 0.01);

    encodedKey = encode(null, EncodeType.KEY, type);
    val = decode(encodedKey, type);
    assertEquals(null, val);
  }
}
