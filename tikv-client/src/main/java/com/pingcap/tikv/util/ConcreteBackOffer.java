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

import com.google.common.base.Preconditions;
import com.pingcap.tikv.exception.GrpcException;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConcreteBackOffer implements BackOff {
  private int attempts;
  private int counter;
  private int maxSleep;
  private int totalSleep;
  private Map<BackoffFunction.BackOffFuncType, BackoffFunction> backoffFunctionMap;
  private List<Exception> errors;
  private static final Logger logger = Logger.getLogger(ConcreteBackOffer.class);

//  public ConcreteBackOffer(int attempts) {
//    Preconditions.checkArgument(attempts >= 1, "Retry count cannot be less than 1.");
//    this.counter = 1;
//    this.attempts = attempts;
//    this.errors = new ArrayList<>();
//  }
  public ConcreteBackOffer(int maxSleep) {
    this.maxSleep = maxSleep;
    this.errors = new ArrayList<>();
    this.backoffFunctionMap = new HashMap<>();
  }

  @Override
  public void doBackOff(BackoffFunction.BackOffFuncType funcType, Exception err) {
    BackoffFunction backoffFunction = backoffFunctionMap.get(funcType);
    if (backoffFunction == null) {
      switch (funcType) {
        case BoUpdateLeader:
          backoffFunction = BackoffFunction.create(1, 10, BackOffStrategy.NoJitter);
          break;
        case boTxnLockFast:
          backoffFunction = BackoffFunction.create(100, 3000, BackOffStrategy.EqualJitter);
          break;
        case boServerBusy:
          backoffFunction = BackoffFunction.create(2000, 10000, BackOffStrategy.EqualJitter);
          break;
        case BoRegionMiss:
          backoffFunction = BackoffFunction.create(100, 500,  BackOffStrategy.NoJitter);
          break;
        case BoTxnLock:
          backoffFunction = BackoffFunction.create(200, 3000, BackOffStrategy.EqualJitter);
          break;
        case boPDRPC:
          backoffFunction = BackoffFunction.create(500, 3000, BackOffStrategy.EqualJitter);
          break;
        case boTiKVRPC:
          backoffFunction = BackoffFunction.create(200, 3000, BackOffStrategy.EqualJitter);
          break;
      }
      backoffFunctionMap.put(funcType, backoffFunction);
    }

    totalSleep += backoffFunction.doBackOff();
    logger.debug(String.format("%s, retry later(totalSleep %dms, maxSleep %dms)", err.getMessage(), totalSleep, maxSleep));
    errors.add(err);
    if (maxSleep > 0 && totalSleep >= maxSleep) {
      StringBuilder errMsg = new StringBuilder(String.format("backoffer.maxSleep %dms is exceeded, errors:", maxSleep));
      for (int i = 0; i < errors.size(); i++) {
        Throwable curErr = errors.get(i);
        // Print only last 3 errors for non-DEBUG log levels.
        if (logger.isDebugEnabled() || i >= errors.size() - 3) {
          errMsg.append("\n").append(curErr.toString());
        }
      }
      logger.warn(errMsg.toString());
      // Use the last backoff type to generate an exception
      throw new GrpcException("retry is exhausted.", err);
    }
  }

  @Override
  public void reset() {
    this.counter = 1;
  }

  /**
   * produces 0 1 1 2 3 ... fibonacci series number.
   */
  @Override
  public long nextBackOffMillis() {
    if (attempts <= counter) {
      return BackOff.STOP;
    }
    long millis = (counter << 2) * 1000;
    counter++;
    return millis;
  }
}
