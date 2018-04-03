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
  void doBackOff(BackOffFunction.BackOffFuncType funcTypes, Exception err);

  /**
   * Fixed back-off policy whose back-off time is always zero, meaning that the operation is retried
   * immediately without waiting.
   */
  BackOff ZERO_BACKOFF = new BackOff() {
    @Override
    public void doBackOff(BackOffFunction.BackOffFuncType funcTypes, Exception err) {
      // Pass
    }
  };
}
