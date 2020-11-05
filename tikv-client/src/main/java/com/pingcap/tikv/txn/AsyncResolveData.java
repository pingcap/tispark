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

import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.ResolveLockException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;

public class AsyncResolveData {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncResolveData.class);

  /**
   * If any key has been committed (missingLock is true), then this is the commit ts. In that case,
   * all locks should be committed with the same commit timestamp. If no locks have been committed
   * (missingLock is false), then we will use max(all min commit ts) from all locks; i.e., it is the
   * commit ts we should use. Note that a secondary lock's commit ts may or may not be the same as
   * the primary lock's min commit ts.
   */
  private long commitTs;

  private List<ByteString> keys;
  private boolean missingLock;

  public AsyncResolveData(long commitTs, List<ByteString> keys, boolean missingLock) {
    this.commitTs = commitTs;
    this.keys = keys;
    this.missingLock = missingLock;
  }

  public long getCommitTs() {
    return commitTs;
  }

  public List<ByteString> getKeys() {
    return keys;
  }

  public synchronized void appendKey(ByteString key) {
    ArrayList<ByteString> newKeys = new ArrayList<>(this.getKeys());
    newKeys.add(key);
    this.keys = newKeys;
  }

  /**
   * addKeys adds the keys from locks to data, keeping other fields up to date. startTS and commitTS
   * are for the transaction being resolved.
   *
   * <p>In the async commit protocol when checking locks, we send a list of keys to check and get
   * back a list of locks. There will be a lock for every key which is locked. If there are fewer
   * locks than keys, then a lock is missing because it has been committed, rolled back, or was
   * never locked.
   *
   * <p>In this function, locks is the list of locks, and expected is the number of keys.
   * asyncResolveData.missingLock will be set to true if the lengths don't match. If the lengths do
   * match, then the locks are added to asyncResolveData.locks and will need to be resolved by the
   * caller.
   */
  public synchronized void addKeys(
      List<Kvrpcpb.LockInfo> locks, int expected, long startTs, long commitTs)
      throws ResolveLockException {
    if (locks.size() < expected) {
      LOG.debug(
          String.format(
              "addKeys: lock has been committed or rolled back, startTs=%d, commitTs=%d",
              startTs, commitTs));

      if (!this.missingLock) {
        // commitTS == 0 => lock has been rolled back.
        if (commitTs != 0 && commitTs < this.commitTs) {
          throw new ResolveLockException(
              String.format(
                  "commit TS must be greater or equal to min commit TS, commitTs=%d, minCommitTs=%d",
                  commitTs, this.commitTs));
        }
        this.commitTs = commitTs;
      }
      this.missingLock = true;

      if (this.commitTs != commitTs) {
        throw new ResolveLockException(
            String.format(
                "commit TS mismatch in async commit recovery: %d and %d", this.commitTs, commitTs));
      }
      // We do not need to resolve the remaining locks because TiKV will have resolved them as
      // appropriate.
    } else {
      LOG.debug(String.format("addKeys: all locks present, startTs=%d", startTs));

      // Save all locks to be resolved.
      for (Kvrpcpb.LockInfo lockInfo : locks) {
        if (lockInfo.getLockVersion() != startTs) {
          throw new ResolveLockException(
              String.format(
                  "unexpected timestamp, expected: %d, found: %d",
                  startTs, lockInfo.getLockVersion()));
        }
        if (!this.missingLock && lockInfo.getMinCommitTs() > this.commitTs) {
          this.commitTs = lockInfo.getMinCommitTs();
        }
        this.appendKey(lockInfo.getKey());
      }
    }
  }
}
