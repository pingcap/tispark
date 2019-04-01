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
  int COP_BUILD_TASK_MAX_BACKOFF = 5000;
  int TSO_MAX_BACKOFF = 5000;
  int SCANNER_NEXT_MAX_BACKOFF = 40000;
  int BATCH_GET_MAX_BACKOFF = 40000;
  int COP_NEXT_MAX_BACKOFF = 40000;
  int GET_MAX_BACKOFF = 40000;
  int PREWRITE_MAX_BACKOFF = 20000;
  int CLEANUP_MAX_BACKOFF = 20000;
  int GC_ONE_REGION_MAX_BACKOFF = 20000;
  int GC_RESOLVE_LOCK_MAX_BACKOFF = 100000;
  int GC_DELETE_RANGE_MAX_BACKOFF = 100000;
  int RAWKV_MAX_BACKOFF = 40000;
  int SPLIT_REGION_BACKOFF = 20000;
  int COMMIT_MAX_BACKOFF = 3000;

  /** 20 seconds */
  int BATCH_PREWRITE_BACKOFF = 20000;

  /** 3 seconds */
  int BATCH_COMMIT_BACKOFF = 3000;

  /**
   * doBackOff sleeps a while base on the BackOffType and records the error message. Will stop until
   * max back off time exceeded and throw an exception to the caller.
   */
  void doBackOff(BackOffFunction.BackOffFuncType funcTypes, Exception err);
}
