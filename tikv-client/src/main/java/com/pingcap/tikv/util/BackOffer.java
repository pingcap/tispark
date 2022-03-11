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
  // Back off types.
  int seconds = 1000;
  int COP_BUILD_TASK_MAX_BACKOFF = 5 * seconds;
  int TSO_MAX_BACKOFF = 5 * seconds;
  int SCANNER_NEXT_MAX_BACKOFF = 40 * seconds;
  int BATCH_GET_MAX_BACKOFF = 40 * seconds;
  int COP_NEXT_MAX_BACKOFF = 40 * seconds;
  int GET_MAX_BACKOFF = 40 * seconds;
  int PREWRITE_MAX_BACKOFF = 20 * seconds;
  int CLEANUP_MAX_BACKOFF = 20 * seconds;
  int GC_ONE_REGION_MAX_BACKOFF = 20 * seconds;
  int GC_RESOLVE_LOCK_MAX_BACKOFF = 100 * seconds;
  int GC_DELETE_RANGE_MAX_BACKOFF = 100 * seconds;
  int RAWKV_MAX_BACKOFF = 40 * seconds;
  int SPLIT_REGION_BACKOFF = 20 * seconds;
  int BATCH_COMMIT_BACKOFF = 10 * seconds;
  int PD_INFO_BACKOFF = 5 * seconds;
  int ROW_ID_ALLOCATOR_BACKOFF = 40 * seconds;

  /**
   * doBackOff sleeps a while base on the BackOffType and records the error message. Will stop until
   * max back off time exceeded and throw an exception to the caller.
   */
  void doBackOff(BackOffFunction.BackOffFuncType funcType, Exception err);

  /**
   * BackoffWithMaxSleep sleeps a while base on the backoffType and records the error message and
   * never sleep more than maxSleepMs for each sleep.
   */
  void doBackOffWithMaxSleep(
      BackOffFunction.BackOffFuncType funcType, long maxSleepMs, Exception err);

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
}
