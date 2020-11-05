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

package com.pingcap.tikv.txn;

import com.google.protobuf.ByteString;
import org.tikv.kvproto.Kvrpcpb;

public class Lock {
  private static final long DEFAULT_LOCK_TTL = 3000;
  private final long txnID;
  private final long ttl;
  private final ByteString key;
  private final ByteString primary;
  private final long txnSize;
  private final Kvrpcpb.Op lockType;
  private final boolean useAsyncCommit;
  private final long lockForUpdateTs;
  private final long minCommitTS;

  public Lock(Kvrpcpb.LockInfo l) {
    txnID = l.getLockVersion();
    key = l.getKey();
    primary = l.getPrimaryLock();
    ttl = l.getLockTtl() == 0 ? DEFAULT_LOCK_TTL : l.getLockTtl();
    txnSize = l.getTxnSize();
    lockType = l.getLockType();
    useAsyncCommit = l.getUseAsyncCommit();
    lockForUpdateTs = l.getLockForUpdateTs();
    minCommitTS = l.getMinCommitTs();
  }

  public long getTxnID() {
    return txnID;
  }

  public long getTtl() {
    return ttl;
  }

  public ByteString getKey() {
    return key;
  }

  public ByteString getPrimary() {
    return primary;
  }

  public long getTxnSize() {
    return txnSize;
  }

  public Kvrpcpb.Op getLockType() {
    return lockType;
  }

  public long getLockForUpdateTs() {
    return lockForUpdateTs;
  }

  public boolean isUseAsyncCommit() {
    return useAsyncCommit;
  }

  public long getMinCommitTS() {
    return minCommitTS;
  }
}
