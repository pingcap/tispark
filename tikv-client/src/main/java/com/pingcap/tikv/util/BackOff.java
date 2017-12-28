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

public interface BackOff {
  /** Indicates that no more retries should be made for use in {@link #nextBackOffMillis()}. */
  long STOP = -1L;
  /** Reset to initial state. */
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
   long backOffMillis = backoff.nextBackOffMillis();
   if (backOffMillis == Backoff.STOP) {
   // do not retry operation
   } else {
   // sleep for backOffMillis milliseconds and retry operation
   }
   * </pre>
   */
  long nextBackOffMillis();

  /**
   * Fixed back-off policy whose back-off time is always zero, meaning that the operation is retried
   * immediately without waiting.
   */
  BackOff ZERO_BACKOFF = new BackOff() {

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

    public void reset() {
    }

    public long nextBackOffMillis() {
      return STOP;
    }
  };
}
