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

import com.pingcap.tikv.util.BackOffFunction.BackOffFuncType;

public interface BackOffer extends org.tikv.common.util.BackOffer {

  int COP_BUILD_TASK_MAX_BACKOFF = 5 * seconds;
  int PREWRITE_MAX_BACKOFF = 20 * seconds;
  int CLEANUP_MAX_BACKOFF = 20 * seconds;
  int GC_ONE_REGION_MAX_BACKOFF = 20 * seconds;
  int GC_RESOLVE_LOCK_MAX_BACKOFF = 100 * seconds;
  int GC_DELETE_RANGE_MAX_BACKOFF = 100 * seconds;
  int BATCH_COMMIT_BACKOFF = 10 * seconds;
  int ROW_ID_ALLOCATOR_BACKOFF = 40 * seconds;

  /**
   * doBackOff sleeps a while base on the BackOffType and records the error message. Will stop until
   * max back off time exceeded and throw an exception to the caller.
   */
  void doBackOff(BackOffFuncType funcType, Exception err);

  /**
   * BackoffWithMaxSleep sleeps a while base on the backoffType and records the error message and
   * never sleep more than maxSleepMs for each sleep.
   */
  void doBackOffWithMaxSleep(BackOffFuncType funcType, long maxSleepMs, Exception err);
}
