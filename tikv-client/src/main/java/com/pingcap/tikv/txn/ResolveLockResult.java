/*
 *
 * Copyright 2020 PingCAP, Inc.
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

package com.pingcap.tikv.txn;

import java.util.HashSet;
import java.util.Set;

public class ResolveLockResult {
  private final long msBeforeTxnExpired;
  private final Set<Long> resolvedLocks;

  public ResolveLockResult(long msBeforeTxnExpired) {
    this.msBeforeTxnExpired = msBeforeTxnExpired;
    this.resolvedLocks = new HashSet<>();
  }

  public ResolveLockResult(long msBeforeTxnExpired, Set<Long> resolvedLocks) {
    this.msBeforeTxnExpired = msBeforeTxnExpired;
    this.resolvedLocks = resolvedLocks;
  }

  public long getMsBeforeTxnExpired() {
    return msBeforeTxnExpired;
  }

  public Set<Long> getResolvedLocks() {
    return resolvedLocks;
  }
}
