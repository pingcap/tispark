/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.util;

import org.tikv.common.exception.GrpcException;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffFunction.BackOffFuncType;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;
import org.tikv.common.util.ConcreteBackOffer;

public class ConcreteBackOffTest {

  private AtomicBoolean failed = new AtomicBoolean(false);

  // If Exception(except GrpcException), the test should be failed
  private void runDoBackOffSafely(
      ConcreteBackOffer concreteBackOffer,
      BackOffFunction.BackOffFuncType funcType,
      Exception err) {
    try {
      concreteBackOffer.doBackOff(funcType, err);
    } catch (GrpcException ignored) {

    } catch (Exception e) {
      e.printStackTrace();
      failed.set(true);
    }
  }

  @Test
  public void TestDoBackOffConcurrency() {
    ConcreteBackOffer concreteBackOffer = ConcreteBackOffer.newCustomBackOff(100);
    CountDownLatch latch = new CountDownLatch(1000);
    BackOffFuncType[] values = BackOffFuncType.values();

    for (int i = 0; i < 1000; i++) {
      int finalI = i;
      new Thread(
              () -> {
                latch.countDown();
                runDoBackOffSafely(
                    concreteBackOffer,
                    values[finalI % values.length],
                    new RuntimeException("test mock"));
              })
          .start();
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // check the result of test
    Assert.assertFalse(failed.get());
  }
}
