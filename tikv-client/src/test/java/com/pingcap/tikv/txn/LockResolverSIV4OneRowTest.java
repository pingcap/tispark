/*
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
 */

package com.pingcap.tikv.txn;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

import com.pingcap.tikv.exception.KeyException;
import java.util.Collections;
import org.junit.Test;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;

public class LockResolverSIV4OneRowTest extends LockResolverTest {
  private final String value1 = "v1";
  private final String value2 = "v2";

  public LockResolverSIV4OneRowTest() {
    super(IsolationLevel.SI);
  }

  @Test
  public void TTLExpire() throws InterruptedException {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isLockResolverClientV4()) {
      skipTestTiDBV4();
      return;
    }

    String key = genRandomKey(64);
    long ttl = GET_BACKOFF - GET_BACKOFF / 2;

    // Put <key, value1> into kv
    putKVandTestGet(key, value1);

    // Prewrite <key, value2> as primary without committing it
    long startTs = session.getTimestamp().getVersion();
    long endTs = session.getTimestamp().getVersion();
    assertTrue(lockKey(key, value2, key, value2, false, startTs, endTs, ttl));

    // TTL expires, we should be able to read <key, value1> instead.
    Thread.sleep(ttl);
    assertEquals(pointGet(key), value1);

    commitFail(key, startTs, endTs);
  }

  @Test
  public void NonBlockingReadTTLExpireCommitFail() throws InterruptedException {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isLockResolverClientV4()) {
      skipTestTiDBV4();
      return;
    }

    String key = genRandomKey(64);
    long ttl = GET_BACKOFF + GET_BACKOFF / 2;

    // Put <key, value1> into kv
    putKVandTestGet(key, value1);

    // Prewrite <key, value2> as primary without committing it
    long startTs = session.getTimestamp().getVersion();
    long endTs = session.getTimestamp().getVersion();
    assertTrue(lockKey(key, value2, key, value2, false, startTs, endTs, ttl));

    // non blocking read
    assertEquals(pointGet(key), value1);

    // TTL expires
    Thread.sleep(ttl);

    // resolve lock
    assertEquals(pointGet(key), value1);

    // commit fail
    commitFail(key, startTs, endTs);
  }

  @Test
  public void NonBlockingReadCommitTSExpiredCommitFail() {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isLockResolverClientV4()) {
      skipTestTiDBV4();
      return;
    }

    String key = genRandomKey(64);
    long ttl = GET_BACKOFF + GET_BACKOFF / 2;

    // Put <key, value1> into kv
    putKVandTestGet(key, value1);

    // Prewrite <key, value2> as primary without committing it
    long startTs = session.getTimestamp().getVersion();
    long endTs = session.getTimestamp().getVersion();
    assertTrue(lockKey(key, value2, key, value2, false, startTs, endTs, ttl));

    // get new endTs
    endTs = session.getTimestamp().getVersion();

    // non blocking read
    assertEquals(pointGet(key), value1);

    try {
      // TTL not expires, but CommitTS expired, commit failed
      commitString(Collections.singletonList(key), startTs, endTs);
      fail();
    } catch (KeyException e) {
      assertTrue(
          e.getMessage().startsWith("Key exception occurred and the reason is commit_ts_expired"));
    }
  }

  @Test
  public void NonBlockingReadCommitSuccess() {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isLockResolverClientV4()) {
      skipTestTiDBV4();
      return;
    }

    String key = genRandomKey(64);
    long ttl = GET_BACKOFF + GET_BACKOFF;

    // Put <key, value1> into kv
    putKVandTestGet(key, value1);

    // Prewrite <key, value2> as primary without committing it
    long startTs = session.getTimestamp().getVersion();
    long endTs = session.getTimestamp().getVersion();
    assertTrue(lockKey(key, value2, key, value2, false, startTs, endTs, ttl));

    // non blocking read
    assertEquals(pointGet(key), value1);

    // get new commitTS
    endTs = session.getTimestamp().getVersion();

    // TTL not expires, CommitTS not expired, commit success
    commitString(Collections.singletonList(key), startTs, endTs);
    assertEquals(pointGet(key), value2);
  }
}
