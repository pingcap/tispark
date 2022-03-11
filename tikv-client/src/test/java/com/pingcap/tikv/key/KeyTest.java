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

package com.pingcap.tikv.key;

import static com.pingcap.tikv.key.Key.toRawKey;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import java.util.Arrays;
import java.util.function.Function;
import org.junit.Test;

public class KeyTest {
  @Test
  public void toKeyTest() throws Exception {
    // compared as unsigned
    testBytes(new byte[] {1, 2, -1, 10}, new byte[] {1, 2, 0, 10}, x -> x > 0);
    testBytes(new byte[] {1, 2, 0, 10}, new byte[] {1, 2, 0, 10}, x -> x == 0);
    testBytes(new byte[] {1, 2, 0, 10}, new byte[] {1, 2, 1, 10}, x -> x < 0);
    testBytes(new byte[] {1, 2, 0, 10}, new byte[] {1, 2, 0}, x -> x > 0);

    testLiteral(1, 2, IntegerType.INT, x -> x < 0);
    testLiteral(13, 13, IntegerType.INT, x -> x == 0);
    testLiteral(13, 2, IntegerType.INT, x -> x > 0);
    testLiteral(-1, 2, IntegerType.INT, x -> x < 0);
  }

  private void testBytes(byte[] lhs, byte[] rhs, Function<Integer, Boolean> tester) {
    ByteString lhsBS = ByteString.copyFrom(lhs);
    ByteString rhsBS = ByteString.copyFrom(rhs);

    Key lhsComp = toRawKey(lhsBS);
    Key rhsComp = toRawKey(rhsBS);

    assertTrue(tester.apply(lhsComp.compareTo(rhsComp)));

    lhsComp = toRawKey(lhs);
    rhsComp = toRawKey(rhs);

    assertTrue(tester.apply(lhsComp.compareTo(rhsComp)));
  }

  private void testLiteral(
      Object lhs, Object rhs, DataType type, Function<Integer, Boolean> tester) {
    Key lhsComp = TypedKey.toTypedKey(lhs, type);
    Key rhsComp = TypedKey.toTypedKey(rhs, type);

    assertTrue(tester.apply(lhsComp.compareTo(rhsComp)));
  }

  @Test
  public void nextPrefixTest() {
    Key k1 = toRawKey(new byte[] {1, 2, 3});
    assertEquals(toRawKey(new byte[] {1, 2, 4}), k1.nextPrefix());

    k1 = toRawKey(new byte[] {UnsignedBytes.MAX_VALUE, UnsignedBytes.MAX_VALUE});
    assertEquals(
        toRawKey(new byte[] {UnsignedBytes.MAX_VALUE, UnsignedBytes.MAX_VALUE, 0}),
        k1.nextPrefix());
  }

  @Test
  public void nextTest() throws Exception {
    Key k1 = toRawKey(new byte[] {1, 2, 3});
    assertEquals(toRawKey(new byte[] {1, 2, 3, 0}), k1.next());

    k1 = toRawKey(new byte[] {UnsignedBytes.MAX_VALUE, UnsignedBytes.MAX_VALUE});
    assertEquals(
        toRawKey(new byte[] {UnsignedBytes.MAX_VALUE, UnsignedBytes.MAX_VALUE, 0}), k1.next());
  }

  @Test
  public void compareToTest() throws Exception {
    Key kNegInf = toRawKey(new byte[0], true);
    Key kMin = Key.MIN;
    Key k = toRawKey(new byte[] {1});
    Key kMax = Key.MAX;
    Key kInf = toRawKey(new byte[0], false);
    Key[] keys = new Key[] {kInf, kMax, k, kMin, kNegInf};
    Arrays.sort(keys);
    assertArrayEquals(new Key[] {kNegInf, kMin, k, kMax, kInf}, keys);
  }
}
