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
  int seconds = 1000;
  int copBuildTaskMaxBackoff = 5 * seconds;
  int tsoMaxBackoff = 5 * seconds;
  int scannerNextMaxBackoff = 40 * seconds;
  int batchGetMaxBackoff = 40 * seconds;
  int copNextMaxBackoff = 40 * seconds;
  int getMaxBackoff = 40 * seconds;
  int prewriteMaxBackoff = 20 * seconds;
  int cleanupMaxBackoff = 20 * seconds;
  int GcOneRegionMaxBackoff = 20 * seconds;
  int GcResolveLockMaxBackoff = 100 * seconds;
  int GcDeleteRangeMaxBackoff = 100 * seconds;
  int rawkvMaxBackoff = 40 * seconds;
  int splitRegionBackoff = 20 * seconds;
  int batchPrewriteBackoff = 20 * seconds;
  int batchCommitBackoff = 3 * seconds;

  /**
   * doBackOff sleeps a while base on the BackOffType and records the error message. Will stop until
   * max back off time exceeded and throw an exception to the caller.
   */
  void doBackOff(BackOffFunction.BackOffFuncType funcTypes, Exception err);
}
