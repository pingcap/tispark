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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class ConcreteBackOffer implements BackOffer {
  private int maxSleep;
  private int totalSleep;
  private Map<BackOffFunction.BackOffFuncType, BackOffFunction> backOffFunctionMap;
  private List<Exception> errors;
  private static final Logger logger = Logger.getLogger(ConcreteBackOffer.class);

  public static ConcreteBackOffer newCustomBackOff(int maxSleep) {
    return new ConcreteBackOffer(maxSleep);
  }

  public static ConcreteBackOffer newScannerNextMaxBackOff() {
    return new ConcreteBackOffer(SCANNER_NEXT_MAX_BACKOFF);
  }

  public static ConcreteBackOffer newBatchGetMaxBackOff() {
    return new ConcreteBackOffer(BATCH_GET_MAX_BACKOFF);
  }

  public static ConcreteBackOffer newCopNextMaxBackOff() {
    return new ConcreteBackOffer(COP_NEXT_MAX_BACKOFF);
  }

  public static ConcreteBackOffer newGetBackOff() {
    return new ConcreteBackOffer(GET_MAX_BACKOFF);
  }

  public static ConcreteBackOffer newWaitScatterRegionBackOff() {
    return new ConcreteBackOffer(WAIT_SCATTER_REGION_FINISH);
  }

  public static ConcreteBackOffer newRawKVBackOff() {
    return new ConcreteBackOffer(RAWKV_MAX_BACKOFF);
  }

  public static ConcreteBackOffer newTsoBackOff() {
    return new ConcreteBackOffer(TSO_MAX_BACKOFF);
  }

  public static ConcreteBackOffer create(BackOffer source) {
    return new ConcreteBackOffer(((ConcreteBackOffer) source));
  }

  private ConcreteBackOffer(int maxSleep) {
    Preconditions.checkArgument(maxSleep >= 0, "Max sleep time cannot be less than 0.");
    this.maxSleep = maxSleep;
    this.errors = new ArrayList<>();
    this.backOffFunctionMap = new HashMap<>();
  }

  private ConcreteBackOffer(ConcreteBackOffer source) {
    this.maxSleep = source.maxSleep;
    this.totalSleep = source.totalSleep;
    this.errors = source.errors;
    this.backOffFunctionMap = source.backOffFunctionMap;
  }

  /**
   * Creates a back off func which implements exponential back off with optional jitters according
   * to different back off strategies. See http://www.awsarchitectureblog.com/2015/03/backoff.html
   */
  private BackOffFunction createBackOffFunc(BackOffFunction.BackOffFuncType funcType) {
    BackOffFunction backOffFunction = null;
    switch (funcType) {
      case BoUpdateLeader:
        backOffFunction = BackOffFunction.create(1, 10, BackOffStrategy.NoJitter);
        break;
      case BoTxnLockFast:
        backOffFunction = BackOffFunction.create(100, 3000, BackOffStrategy.EqualJitter);
        break;
      case BoServerBusy:
        backOffFunction = BackOffFunction.create(2000, 10000, BackOffStrategy.EqualJitter);
        break;
      case BoRegionMiss:
        backOffFunction = BackOffFunction.create(100, 500, BackOffStrategy.NoJitter);
        break;
      case BoTxnLock:
        backOffFunction = BackOffFunction.create(200, 3000, BackOffStrategy.EqualJitter);
        break;
      case BoPDRPC:
        backOffFunction = BackOffFunction.create(500, 3000, BackOffStrategy.EqualJitter);
        break;
      case BoTiKVRPC:
        backOffFunction = BackOffFunction.create(100, 2000, BackOffStrategy.EqualJitter);
        break;
    }
    return backOffFunction;
  }

  @Override
  public void doBackOff(BackOffFunction.BackOffFuncType funcType, Exception err) {
    BackOffFunction backOffFunction =
        backOffFunctionMap.computeIfAbsent(funcType, this::createBackOffFunc);
    //    if (err.getMessage().contains("UNKNOWN")) {
    //      try {
    //        Method method = PlatformDependent.class.getMethod("usedDirectMemory");
    //        method.setAccessible(true);
    //        AtomicLong c = ((AtomicLong) method.invoke(PlatformDependent.class));
    //        System.out.println("direct memory: " + c.get());
    //      } catch (Exception e) {;
    //      }
    //      err.printStackTrace();
    //    }

    // Back off will be done here
    totalSleep += backOffFunction.doBackOff();
    logger.debug(
        String.format(
            "%s, retry later(totalSleep %dms, maxSleep %dms)",
            err.getMessage(), totalSleep, maxSleep));
    errors.add(err);
    if (maxSleep > 0 && totalSleep >= maxSleep) {
      StringBuilder errMsg =
          new StringBuilder(
              String.format("BackOffer.maxSleep %dms is exceeded, errors:", maxSleep));
      for (int i = 0; i < errors.size(); i++) {
        Exception curErr = errors.get(i);
        // Print only last 3 errors for non-DEBUG log levels.
        if (logger.isDebugEnabled() || i >= errors.size() - 3) {
          errMsg.append("\n").append(i).append(".").append(curErr.toString());
        }
      }
      logger.warn(errMsg.toString());
      // Use the last backoff type to generate an exception
      throw new GrpcException("retry is exhausted.", err);
    }
  }
}
