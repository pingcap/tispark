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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.pingcap.tikv.types.IntegerType;
import org.junit.Test;

public class IndexKeyTest {

  @Test
  public void createTest() {
    Key k1 = Key.toRawKey(new byte[] {1, 2, 3, 4});
    Key k2 = Key.toRawKey(new byte[] {5, 6, 7, 8});
    Key k3 = Key.toRawKey(new byte[] {5, 6, 7, 9});
    IndexKey ik1 = IndexKey.toIndexKey(666, 777, k1, k2);
    IndexKey ik2 = IndexKey.toIndexKey(666, 777, k1, k2);
    IndexKey ik3 = IndexKey.toIndexKey(666, 777, k1, k3);
    assertEquals(ik1, ik2);
    assertEquals(0, ik1.compareTo(ik2));
    assertTrue(ik1.compareTo(ik3) < 0);
    assertEquals(2, ik1.getDataKeys().length);
    assertEquals(k1, ik1.getDataKeys()[0]);
    assertEquals(k2, ik1.getDataKeys()[1]);

    try {
      IndexKey.toIndexKey(0, 0, k1, null, k2);
      fail();
    } catch (Exception e) {
    }
  }

  @Test
  public void toStringTest() {
    Key k1 = Key.toRawKey(new byte[] {1, 2, 3, 4});
    TypedKey k2 = TypedKey.toTypedKey(666, IntegerType.INT);
    IndexKey ik = IndexKey.toIndexKey(0, 0, k1, Key.NULL, k2);
    assertArrayEquals(ik.getDataKeys()[0].getBytes(), new byte[] {1, 2, 3, 4});
    assertArrayEquals(ik.getDataKeys()[1].getBytes(), new byte[] {0});
    assertArrayEquals(ik.getDataKeys()[2].getBytes(), new byte[] {3, -128, 0, 0, 0, 0, 0, 2, -102});
  }
}
