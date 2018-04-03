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
  private Map<BackOffFunction.BackOffFuncType, BackOffFunction> backoffFunctionMap;
  private List<Exception> errors;
  private static final Logger logger = Logger.getLogger(ConcreteBackOffer.class);

  public static ConcreteBackOffer newCustomBackOff(int maxSleep) {
    return new ConcreteBackOffer(maxSleep);
  }

  public static ConcreteBackOffer newScannerNextMaxBackOff() {
    return new ConcreteBackOffer(scannerNextMaxBackoff);
  }

  public static ConcreteBackOffer newBatchGetMaxBackOff() {
    return new ConcreteBackOffer(batchGetMaxBackoff);
  }

  public static ConcreteBackOffer newCopNextMaxBackOff() {
    return new ConcreteBackOffer(copNextMaxBackoff);
  }

  public static ConcreteBackOffer newGetBackOff() {
    return new ConcreteBackOffer(getMaxBackoff);
  }

  public static ConcreteBackOffer newRawKVBackOff() {
    return new ConcreteBackOffer(rawkvMaxBackoff);
  }

  public static ConcreteBackOffer newTsoBackOff() {
    return new ConcreteBackOffer(tsoMaxBackoff);
  }

  private ConcreteBackOffer(int maxSleep) {
    Preconditions.checkArgument(maxSleep >= 0, "Max sleep time cannot be less than 0.");
    this.maxSleep = maxSleep;
    this.errors = new ArrayList<>();
    this.backoffFunctionMap = new HashMap<>();
  }

  @Override
  public void doBackOff(BackOffFunction.BackOffFuncType funcType, Exception err) {
    BackOffFunction backOffFunction = backoffFunctionMap.get(funcType);
    if (backOffFunction == null) {
      switch (funcType) {
        case BoUpdateLeader:
          backOffFunction = BackOffFunction.create(1, 10, BackOffStrategy.NoJitter);
          break;
        case boTxnLockFast:
          backOffFunction = BackOffFunction.create(100, 3000, BackOffStrategy.EqualJitter);
          break;
        case boServerBusy:
          backOffFunction = BackOffFunction.create(2000, 10000, BackOffStrategy.EqualJitter);
          break;
        case BoRegionMiss:
          backOffFunction = BackOffFunction.create(100, 500, BackOffStrategy.NoJitter);
          break;
        case BoTxnLock:
          backOffFunction = BackOffFunction.create(200, 3000, BackOffStrategy.EqualJitter);
          break;
        case boPDRPC:
          backOffFunction = BackOffFunction.create(500, 3000, BackOffStrategy.EqualJitter);
          break;
        case boTiKVRPC:
          backOffFunction = BackOffFunction.create(200, 3000, BackOffStrategy.EqualJitter);
          break;
      }
      backoffFunctionMap.put(funcType, backOffFunction);
    }

    totalSleep += backOffFunction.doBackOff();
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
}
