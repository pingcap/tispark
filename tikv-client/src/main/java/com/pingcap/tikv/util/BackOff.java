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

import com.pingcap.tikv.exception.GrpcException;

public interface BackOff {
  /**
   * Indicates that no more retries should be made for use in {@link #nextBackOffMillis()}.
   */
  long STOP = -1L;

  enum BackOffStrategy {
    // NoJitter makes the backoff sequence strict exponential.
    NoJitter,
    // FullJitter applies random factors to strict exponential.
    FullJitter,
    // EqualJitter is also randomized, but prevents very short sleeps.
    EqualJitter,
    // DecorrJitter increases the maximum jitter based on the last random value.
    DecorrJitter
  }

  int copBuildTaskMaxBackoff  = 5000;
  int tsoMaxBackoff           = 5000;
  int scannerNextMaxBackoff   = 20000;
  int batchGetMaxBackoff      = 20000;
  int copNextMaxBackoff       = 20000;
  int getMaxBackoff           = 20000;
  int prewriteMaxBackoff      = 20000;
  int cleanupMaxBackoff       = 20000;
  int GcOneRegionMaxBackoff   = 20000;
  int GcResolveLockMaxBackoff = 100000;
  int GcDeleteRangeMaxBackoff = 100000;
  int rawkvMaxBackoff         = 20000;
  int splitRegionBackoff      = 20000;

  /**
   * doBackoff sleeps a while base on the BackOffType and records the error message.
   */
  void doBackOff(BackoffFunction.BackOffFuncType funcTypes, Exception err);

  /**
   * Reset to initial state.
   */
  void reset();

  /**
   * Gets the number of milliseconds to wait before retrying the operation or {@link #STOP} to
   * indicate that no retries should be made.
   *
   * <p>
   * Example usage:
   * </p>
   *
   * <pre>
   * long backOffMillis = backoff.nextBackOffMillis();
   * if (backOffMillis == Backoff.STOP) {
   * // do not retry operation
   * } else {
   * // sleep for backOffMillis milliseconds and retry operation
   * }
   * </pre>
   */
  long nextBackOffMillis();

  /**
   * Fixed back-off policy whose back-off time is always zero, meaning that the operation is retried
   * immediately without waiting.
   */
  BackOff ZERO_BACKOFF = new BackOff() {

    @Override
    public void doBackOff(BackoffFunction.BackOffFuncType funcTypes, Exception err) {

    }

    public void reset() {
    }

    public long nextBackOffMillis() {
      return 0;
    }
  };

  /**
   * Fixed back-off policy that always returns {@code #STOP} for {@link #nextBackOffMillis()},
   * meaning that the operation should not be retried.
   */
  BackOff STOP_BACKOFF = new BackOff() {

    @Override
    public void doBackOff(BackoffFunction.BackOffFuncType funcTypes, Exception err) {
      throw new RuntimeException("Should stop");
    }

    public void reset() {
    }

    public long nextBackOffMillis() {
      return STOP;
    }
  };

  default void doWait(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new GrpcException(e);
    }
  }
}
