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

import java.util.Collections;
import org.junit.Test;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;

public class LockResolverSIV3OneRowTest extends LockResolverTest {
  private String value1 = "v1";
  private String value2 = "v2";

  public LockResolverSIV3OneRowTest() {
    super(IsolationLevel.SI);
  }

  @Test
  public void TTLExpire() {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isV3()) {
      skipTestV3();
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
    assertEquals(pointGet(key), value1);

    commitFail(key, startTs, endTs);
  }

  @Test
  public void TTLNotExpireCommitFail() throws InterruptedException {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isV3()) {
      skipTestV3();
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

    // TTL not expire, resolved key fail
    checkTTLNotExpired(key);

    // TTL expires
    // We should be able to read <key, value1> instead.
    Thread.sleep(ttl);
    assertEquals(pointGet(key), value1);

    commitFail(key, startTs, endTs);
  }

  @Test
  public void TTLNotExpireCommitSuccess() {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isV3()) {
      skipTestV3();
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

    // TTL not expire, resolved key fail
    checkTTLNotExpired(key);

    // continue the commit phase of <key, value2>
    commit(Collections.singletonList(key), startTs, endTs);

    // get
    assertEquals(pointGet(key), value2);
  }
}
