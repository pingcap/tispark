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

public interface BackOffer {
  // Back off strategies
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

  // Back off types.
  int copBuildTaskMaxBackoff  = 5000;
  int tsoMaxBackoff           = 5000;
  int scannerNextMaxBackoff   = 40000;
  int batchGetMaxBackoff      = 40000;
  int copNextMaxBackoff       = 40000;
  int getMaxBackoff           = 40000;
  int prewriteMaxBackoff      = 20000;
  int cleanupMaxBackoff       = 20000;
  int GcOneRegionMaxBackoff   = 20000;
  int GcResolveLockMaxBackoff = 100000;
  int GcDeleteRangeMaxBackoff = 100000;
  int rawkvMaxBackoff         = 40000;
  int splitRegionBackoff      = 20000;

  /**
   * doBackOff sleeps a while base on the BackOffType and records the error message.
   * Will stop until max back off time exceeded and throw an exception to the caller.
   */
  void doBackOff(BackOffFunction.BackOffFuncType funcTypes, Exception err);
}
