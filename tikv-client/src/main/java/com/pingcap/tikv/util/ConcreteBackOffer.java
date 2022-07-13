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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.pingcap.tikv.util;

import static org.tikv.common.ConfigUtils.TIKV_BO_REGION_MISS_BASE_IN_MS;

import com.google.common.base.Preconditions;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiKVException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.log.SlowLog;
import org.tikv.common.log.SlowLogEmptyImpl;
import org.tikv.common.log.SlowLogSpan;
import org.tikv.common.util.BackOffFunction.BackOffFuncType;
import org.tikv.common.util.HistogramUtils;
import org.tikv.shade.com.google.common.annotations.VisibleForTesting;
import org.tikv.shade.io.prometheus.client.Histogram;

public class ConcreteBackOffer implements BackOffer {

  private static final Logger logger = LoggerFactory.getLogger(ConcreteBackOffer.class);
  private final int maxSleep;
  private final Long clusterId;

  @VisibleForTesting public final Map<BackOffFuncType, BackOffFunction> backOffFunctionMap;

  @VisibleForTesting public final List<Exception> errors;
  private int totalSleep;
  private final long deadline;
  private final SlowLog slowLog;

  public static final Histogram BACKOFF_DURATION =
      HistogramUtils.buildDuration()
          .name("client_java_backoff_duration")
          .help("backoff duration.")
          .labelNames("type", "cluster")
          .register();

  private ConcreteBackOffer(int maxSleep, long deadline, SlowLog slowLog, long clusterId) {
    Preconditions.checkArgument(
        maxSleep == 0 || deadline == 0, "Max sleep time should be 0 or Deadline should be 0.");
    Preconditions.checkArgument(maxSleep >= 0, "Max sleep time cannot be less than 0.");
    Preconditions.checkArgument(deadline >= 0, "Deadline cannot be less than 0.");
    this.clusterId = clusterId;
    this.maxSleep = maxSleep;
    this.errors = Collections.synchronizedList(new ArrayList<>());
    this.backOffFunctionMap = new ConcurrentHashMap<>();
    this.deadline = deadline;
    this.slowLog = slowLog;
  }

  private ConcreteBackOffer(ConcreteBackOffer source) {
    this.clusterId = source.clusterId;
    this.maxSleep = source.maxSleep;
    this.totalSleep = source.totalSleep;
    this.errors = source.errors;
    this.backOffFunctionMap = source.backOffFunctionMap;
    this.deadline = source.deadline;
    this.slowLog = source.slowLog;
  }

  public static ConcreteBackOffer newDeadlineBackOff(
      int timeoutInMs, SlowLog slowLog, long clusterId) {
    long deadline = System.currentTimeMillis() + timeoutInMs;
    return new ConcreteBackOffer(0, deadline, slowLog, clusterId);
  }

  public static ConcreteBackOffer newDeadlineBackOff(int timeoutInMs, SlowLog slowLog) {
    return newDeadlineBackOff(timeoutInMs, slowLog, 0);
  }

  public static ConcreteBackOffer newCustomBackOff(int maxSleep, long clusterId) {
    return new ConcreteBackOffer(maxSleep, 0, SlowLogEmptyImpl.INSTANCE, clusterId);
  }

  public static ConcreteBackOffer newCustomBackOff(int maxSleep) {
    return newCustomBackOff(maxSleep, 0);
  }

  public static ConcreteBackOffer newScannerNextMaxBackOff() {
    return new ConcreteBackOffer(SCANNER_NEXT_MAX_BACKOFF, 0, SlowLogEmptyImpl.INSTANCE, 0);
  }

  public static ConcreteBackOffer newBatchGetMaxBackOff() {
    return new ConcreteBackOffer(BATCH_GET_MAX_BACKOFF, 0, SlowLogEmptyImpl.INSTANCE, 0);
  }

  public static ConcreteBackOffer newCopNextMaxBackOff() {
    return newCopNextMaxBackOff(0);
  }

  public static ConcreteBackOffer newCopNextMaxBackOff(long clusterId) {
    return new ConcreteBackOffer(COP_NEXT_MAX_BACKOFF, 0, SlowLogEmptyImpl.INSTANCE, clusterId);
  }

  public static ConcreteBackOffer newGetBackOff(long clusterId) {
    return new ConcreteBackOffer(GET_MAX_BACKOFF, 0, SlowLogEmptyImpl.INSTANCE, clusterId);
  }

  public static ConcreteBackOffer newRawKVBackOff(long clusterId) {
    return new ConcreteBackOffer(RAWKV_MAX_BACKOFF, 0, SlowLogEmptyImpl.INSTANCE, clusterId);
  }

  public static ConcreteBackOffer newRawKVBackOff() {
    return newRawKVBackOff(0);
  }

  public static ConcreteBackOffer newTsoBackOff(long clusterId) {
    return new ConcreteBackOffer(TSO_MAX_BACKOFF, 0, SlowLogEmptyImpl.INSTANCE, clusterId);
  }

  public static ConcreteBackOffer create(BackOffer source) {
    return new ConcreteBackOffer(((ConcreteBackOffer) source));
  }

  /**
   * Creates a back off func which implements exponential back off with optional jitters according
   * to different back off strategies. See http://www.awsarchitectureblog.com/2015/03/backoff.html
   */
  private BackOffFunction createBackOffFunc(BackOffFunction.BackOffFuncType funcType) {
    if (funcType == BackOffFunction.BackOffFuncType.BoRegionMiss) {
      return BackOffFunction.create(
          TiConfiguration.getInt(TIKV_BO_REGION_MISS_BASE_IN_MS), 500, BackOffStrategy.NoJitter);
    }
    return createBackOffFunc(ConverterUpstream.convertBackOffFunctionType(funcType));
  }

  /**
   * Creates a back off func which implements exponential back off with optional jitters according
   * to different back off strategies. See http://www.awsarchitectureblog.com/2015/03/backoff.html
   */
  private BackOffFunction createBackOffFunc(BackOffFuncType funcType) {
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
        backOffFunction =
            BackOffFunction.create(
                TiConfiguration.getInt(TIKV_BO_REGION_MISS_BASE_IN_MS),
                500,
                BackOffStrategy.NoJitter);
        break;
      case BoTxnLock:
        backOffFunction = BackOffFunction.create(200, 3000, BackOffStrategy.EqualJitter);
        break;
      case BoPDRPC:
        backOffFunction = BackOffFunction.create(100, 600, BackOffStrategy.EqualJitter);
        break;
      case BoTiKVRPC:
        backOffFunction = BackOffFunction.create(10, 400, BackOffStrategy.EqualJitter);
        break;
      case BoTxnNotFound:
        backOffFunction = BackOffFunction.create(2, 500, BackOffStrategy.NoJitter);
        break;
      case BoCheckHealth:
        backOffFunction = BackOffFunction.create(100, 600, BackOffStrategy.EqualJitter);
        break;
    }
    return backOffFunction;
  }

  @Override
  public void doBackOff(BackOffFunction.BackOffFuncType funcType, Exception e) {
    doBackOff(ConverterUpstream.convertBackOffFunctionType(funcType), e);
  }

  @Override
  public void doBackOff(BackOffFuncType funcType, Exception e) {
    doBackOffWithMaxSleep(funcType, -1, e);
  }

  @Override
  public void checkTimeout() {
    if (!canRetryAfterSleep(BackOffFuncType.BoCheckTimeout)) {
      logThrowError(new TiKVException("Request Timeout"));
    }
  }

  private void logThrowError(Exception err) {
    StringBuilder errMsg = new StringBuilder();
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

  @Override
  public boolean canRetryAfterSleep(BackOffFuncType funcType) {
    return canRetryAfterSleep(funcType, -1);
  }

  public boolean canRetryAfterSleep(BackOffFuncType funcType, long maxSleepMs) {
    String[] labels = new String[] {funcType.name(), clusterId.toString()};
    Histogram.Timer backOffTimer = BACKOFF_DURATION.labels(labels).startTimer();
    SlowLogSpan slowLogSpan = getSlowLog().start("backoff");
    slowLogSpan.addProperty("type", funcType.name());
    BackOffFunction backOffFunction =
        backOffFunctionMap.computeIfAbsent(funcType, this::createBackOffFunc);

    // Back off will not be done here
    long sleep = backOffFunction.getSleepMs(maxSleepMs);
    totalSleep += sleep;
    // Check deadline
    if (deadline > 0) {
      long currentMs = System.currentTimeMillis();
      if (currentMs + sleep >= deadline) {
        logger.warn(String.format("Deadline %d is exceeded, errors:", deadline));
        slowLogSpan.end();
        backOffTimer.observeDuration();
        return false;
      }
    }

    try {
      Thread.sleep(sleep);
    } catch (InterruptedException e) {
      throw new GrpcException(e);
    } finally {
      slowLogSpan.end();
      backOffTimer.observeDuration();
    }
    if (maxSleep > 0 && totalSleep >= maxSleep) {
      logger.warn(String.format("BackOffer.maxSleep %dms is exceeded, errors:", maxSleep));
      return false;
    }
    return true;
  }

  @Override
  public void doBackOffWithMaxSleep(
      BackOffFunction.BackOffFuncType funcType, long maxSleepMs, Exception err) {
    doBackOffWithMaxSleep(ConverterUpstream.convertBackOffFunctionType(funcType), maxSleepMs, err);
  }

  @Override
  public void doBackOffWithMaxSleep(BackOffFuncType funcType, long maxSleepMs, Exception err) {
    logger.debug(
        String.format(
            "%s, retry later(totalSleep %dms, maxSleep %dms)",
            err.getMessage(), totalSleep, maxSleep));
    errors.add(err);
    if (!canRetryAfterSleep(funcType, maxSleepMs)) {
      logThrowError(err);
    }
  }

  @Override
  public SlowLog getSlowLog() {
    return slowLog;
  }

  public Long getClusterId() {
    return clusterId;
  }
}
