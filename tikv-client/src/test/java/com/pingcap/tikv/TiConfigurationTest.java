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

package com.pingcap.tikv;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class TiConfigurationTest {
  @Test
  public void testListToString() {
    List<String> list = new ArrayList<>();

    list.add("1");
    assertEquals(TiConfiguration.listToString(list), "[1]");

    list.add("2");
    assertEquals(TiConfiguration.listToString(list), "[1,2]");

    list.add("3");
    assertEquals(TiConfiguration.listToString(list), "[1,2,3]");
  }
}
