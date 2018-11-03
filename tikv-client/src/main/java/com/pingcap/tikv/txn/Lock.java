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
import com.pingcap.tikv.kvproto.Kvrpcpb;

public class Lock {
  private final long txnID;
  private final long ttl;
  private final ByteString key;
  private final ByteString primary;
  private static final long defaultLockTTL = 3000;

  public Lock(Kvrpcpb.LockInfo l) {
    txnID = l.getLockVersion();
    key = l.getKey();
    primary = l.getPrimaryLock();
    ttl = l.getLockTtl() == 0 ? defaultLockTTL : l.getLockTtl();
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
}
