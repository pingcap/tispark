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

package com.pingcap.tikv.key;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class CompoundKeyTest {

  @Test
  public void concatTest() {
    Key k1 = Key.toRawKey(new byte[] {1, 2, 3});
    Key k2 = Key.toRawKey(new byte[] {4, 5, 6});
    CompoundKey cpk1 = CompoundKey.concat(k1, k2);
    Key k3 = Key.toRawKey(new byte[] {4});
    Key k4 = Key.toRawKey(new byte[] {5, 6});

    assertEquals(Key.toRawKey(new byte[] {1, 2, 3, 4, 5, 6}), cpk1);

    CompoundKey cpk2 = CompoundKey.concat(k1, k3);
    CompoundKey cpk3 = CompoundKey.concat(cpk2, k4);
    assertEquals(cpk1, cpk3);
  }

  @Test
  public void getKeysTest() {
    Key k1 = Key.toRawKey(new byte[] {1, 2, 3});
    Key k2 = Key.toRawKey(new byte[] {4, 5, 6});
    Key k3 = Key.toRawKey(new byte[] {7, 8, 9});
    CompoundKey.Builder b1 = CompoundKey.newBuilder();
    CompoundKey cpk1 = b1.append(k1).append(k2).build();
    assertEquals(2, cpk1.getKeys().size());
    assertEquals(k1, cpk1.getKeys().get(0));
    assertEquals(k2, cpk1.getKeys().get(1));
    assertArrayEquals(cpk1.getKeys().get(0).getBytes(), new byte[] {1, 2, 3});
    assertArrayEquals(cpk1.getKeys().get(1).getBytes(), new byte[] {4, 5, 6});

    CompoundKey.Builder b2 = CompoundKey.newBuilder();
    CompoundKey cpk2 = b2.append(cpk1).append(k3).build();
    assertEquals(3, cpk2.getKeys().size());
    assertEquals(k1, cpk2.getKeys().get(0));
    assertEquals(k2, cpk2.getKeys().get(1));
    assertEquals(k3, cpk2.getKeys().get(2));
    assertArrayEquals(cpk2.getKeys().get(0).getBytes(), new byte[] {1, 2, 3});
    assertArrayEquals(cpk2.getKeys().get(1).getBytes(), new byte[] {4, 5, 6});
    assertArrayEquals(cpk2.getKeys().get(2).getBytes(), new byte[] {7, 8, 9});
  }
}
