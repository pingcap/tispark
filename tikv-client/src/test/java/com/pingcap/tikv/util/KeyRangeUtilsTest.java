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

package com.pingcap.tikv.util;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import org.junit.Test;

import java.util.List;

import static com.pingcap.tikv.util.KeyRangeUtils.makeCoprocRange;
import static org.junit.Assert.assertEquals;

public class KeyRangeUtilsTest {

  @Test
  public void split() throws Exception {
    ByteString s = ByteString.copyFrom(new byte[] {1,2,3,5});
    ByteString e = ByteString.copyFrom(new byte[] {1,2,3,8});
    List<KeyRange> result = KeyRangeUtils.split(makeCoprocRange(s, e), 2);

    assertEquals(2, result.size());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,5}), result.get(0).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,6, (byte)128}), result.get(0).getEnd());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,6, (byte)128}), result.get(1).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,8}), result.get(1).getEnd());

    s = ByteString.copyFrom(new byte[] {1,2,3,5});
    e = ByteString.copyFrom(new byte[] {1,2,3,6});
    result = KeyRangeUtils.split(makeCoprocRange(s, e), 4);

    assertEquals(4, result.size());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,5}), result.get(0).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,5, (byte)64}), result.get(0).getEnd());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,5, (byte)64}), result.get(1).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,5, (byte)128}), result.get(1).getEnd());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,5, (byte)128}), result.get(2).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,5, (byte)192}), result.get(2).getEnd());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,5, (byte)192}), result.get(3).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,6}), result.get(3).getEnd());

    s = ByteString.copyFrom(new byte[] {1,2,3,5});
    e = ByteString.copyFrom(new byte[] {1,2,3,5});
    result = KeyRangeUtils.split(makeCoprocRange(s, e), 4);
    assertEquals(1, result.size());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,5}), result.get(0).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,5}), result.get(0).getEnd());

    s = ByteString.copyFrom(new byte[] {1,2,3});
    e = ByteString.copyFrom(new byte[] {1,2,3,5});
    result = KeyRangeUtils.split(makeCoprocRange(s, e), 4);
    assertEquals(4, result.size());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3}), result.get(0).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,1, (byte)64}), result.get(0).getEnd());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,1, (byte)64}), result.get(1).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,2, (byte)128}), result.get(1).getEnd());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,2, (byte)128}), result.get(2).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,3, (byte)192}), result.get(2).getEnd());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,3, (byte)192}), result.get(3).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,5}), result.get(3).getEnd());

    s = ByteString.copyFrom(new byte[] {1,2,3,0});
    e = ByteString.copyFrom(new byte[] {1,2,3});
    result = KeyRangeUtils.split(makeCoprocRange(s, e), 4);
    assertEquals(1, result.size());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,0}), result.get(0).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3}), result.get(0).getEnd());

    s = ByteString.EMPTY;
    e = ByteString.copyFrom(new byte[] {1,2,3});
    result = KeyRangeUtils.split(makeCoprocRange(s, e), 4);
    assertEquals(1, result.size());

    assertEquals(ByteString.EMPTY, result.get(0).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3}), result.get(0).getEnd());


    s = ByteString.copyFrom(new byte[] {1,2,3});
    e = ByteString.copyFrom(new byte[] {1,2,3,0,0,0});
    result = KeyRangeUtils.split(makeCoprocRange(s, e), 4);
    assertEquals(1, result.size());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3}), result.get(0).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,0,0,0}), result.get(0).getEnd());

    s = ByteString.copyFrom(new byte[] {1,2,3});
    e = ByteString.copyFrom(new byte[] {1,2,3,1,-128,1});
    result = KeyRangeUtils.split(makeCoprocRange(s, e), 4);
    assertEquals(4, result.size());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3}), result.get(0).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,0,96}), result.get(0).getEnd());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,0,96}), result.get(1).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,0,(byte)192}), result.get(1).getEnd());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,0,(byte)192}), result.get(2).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,1,(byte)32}), result.get(2).getEnd());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,1,(byte)32}), result.get(3).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,1,(byte)128,1}), result.get(3).getEnd());

    s = ByteString.copyFrom(new byte[] {1,2,3});
    e = ByteString.copyFrom(new byte[] {1,2,3,0,0,1});
    result = KeyRangeUtils.split(makeCoprocRange(s, e), 4);
    assertEquals(4, result.size());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3}), result.get(0).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,0,0,0,64}), result.get(0).getEnd());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,0,0,0,64}), result.get(1).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,0,0,0,(byte)128}), result.get(1).getEnd());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,0,0,0,(byte)128}), result.get(2).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,0,0,0,(byte)192}), result.get(2).getEnd());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,0,0,0,(byte)192}), result.get(3).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,0,0,1}), result.get(3).getEnd());

    /** for now only larger diff will be counted*/
    s = ByteString.copyFrom(new byte[] {1,2,3,4,(byte)255});
    e = ByteString.copyFrom(new byte[] {1,2,3,5});
    result = KeyRangeUtils.split(makeCoprocRange(s, e), 1);
    assertEquals(1, result.size());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,4,(byte)255}), result.get(0).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,5}), result.get(0).getEnd());

    s = ByteString.copyFrom(new byte[] {1,2,3,5});
    e = ByteString.copyFrom(new byte[] {1,2,4,5});
    result = KeyRangeUtils.split(makeCoprocRange(s, e), 4);
    assertEquals(4, result.size());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,5}), result.get(0).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,(byte)69}), result.get(0).getEnd());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,(byte)69}), result.get(1).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,(byte)133}), result.get(1).getEnd());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,(byte)133}), result.get(2).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,(byte)197}), result.get(2).getEnd());

    assertEquals(ByteString.copyFrom(new byte[] {1,2,3,(byte)197}), result.get(3).getStart());
    assertEquals(ByteString.copyFrom(new byte[] {1,2,4,5}), result.get(3).getEnd());
  }

}