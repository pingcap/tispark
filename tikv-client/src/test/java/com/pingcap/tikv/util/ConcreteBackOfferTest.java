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

package com.pingcap.tikv.util;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class ConcreteBackOfferTest {
  @Test
  public void nextBackOffMillisTest() {
    BackOff backOff = new ConcreteBackOffer(10);
    for(int i = 1; i < 10; i++) {
      long nextBackoffMillis = backOff.nextBackOffMillis();
      int factor = i<<2;
      assertEquals(nextBackoffMillis, factor*1000);
    }
  }

}
