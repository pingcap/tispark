/*
 * Copyright 2019 PingCAP, Inc.
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

import static com.pingcap.tikv.codec.KeyUtils.formatBytes;
import static com.pingcap.tikv.codec.KeyUtils.hasPrefix;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import org.junit.Test;
import org.tikv.kvproto.Coprocessor;

public class KeyUtilsTest {
  @Test
  public void testFormatBytes() {
    byte[] bytes = null;
    assertEquals(formatBytes(bytes), "null");
    bytes = new byte[] {0, 1, 2, 3};
    assertEquals(formatBytes(bytes), "0,1,2,3");
    ByteString byteString = null;
    assertEquals(formatBytes(byteString), "null");
    byteString = ByteString.EMPTY;
    assertEquals(formatBytes(byteString), "");
    byteString = ByteString.copyFrom(bytes);
    assertEquals(formatBytes(byteString), "0,1,2,3");
    Coprocessor.KeyRange keyRange =
        Coprocessor.KeyRange.newBuilder().setStart(ByteString.EMPTY).setEnd(byteString).build();
    assertEquals(formatBytes(keyRange), "([], [0,1,2,3])");
    keyRange =
        Coprocessor.KeyRange.newBuilder()
            .setStart(byteString)
            .setEnd(ByteString.copyFrom(new byte[] {0, 1, 3, 4}))
            .build();
    assertEquals(formatBytes(keyRange), "([0,1,2,3], [0,1,3,4])");
  }

  @Test
  public void testHasPrefix() {
    ByteString text = ByteString.EMPTY;
    ByteString prefix = ByteString.copyFrom(new byte[] {0, 1, 2});
    assertFalse(hasPrefix(text, prefix));
    text = ByteString.copyFrom(new byte[] {0, 1, 2});
    assertTrue(hasPrefix(text, prefix));
    text = ByteString.copyFrom(new byte[] {0, 1, 2, 3});
    assertTrue(hasPrefix(text, prefix));
    text = ByteString.copyFrom(new byte[] {0, 1, 1});
    assertFalse(hasPrefix(text, prefix));
  }
}
