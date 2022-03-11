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

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

import com.pingcap.tikv.exception.KeyException;
import java.util.Collections;
import junit.framework.TestCase;
import org.junit.Test;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;

public class LockResolverSIV4TwoRowTest extends LockResolverTest {
  private final String value1 = "v1";
  private final String value2 = "v2";
  private final String value3 = "v3";
  private final String value4 = "v4";

  public LockResolverSIV4TwoRowTest() {
    super(IsolationLevel.SI);
  }

  @Test
  public void prewriteCommitSuccessTest() {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isLockResolverClientV4()) {
      skipTestTiDBV4();
      return;
    }

    String primaryKey = genRandomKey(64);
    String secondaryKey = genRandomKey(64);

    long startTs = session.getTimestamp().getVersion();
    long endTs = session.getTimestamp().getVersion();

    // prewriteString <primary key, value1>
    assertTrue(prewriteString(primaryKey, value1, startTs, primaryKey, DEFAULT_TTL));

    // prewriteString <secondary key, value2>
    assertTrue(prewriteString(secondaryKey, value2, startTs, primaryKey, DEFAULT_TTL));

    // commitString primary key
    assertTrue(commitString(Collections.singletonList(primaryKey), startTs, endTs));

    // commitString secondary key
    assertTrue(commitString(Collections.singletonList(secondaryKey), startTs, endTs));

    // get check primary key & secondary key
    assertEquals(pointGet(primaryKey), value1);
    assertEquals(pointGet(secondaryKey), value2);
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

    String primaryKey = genRandomKey(64);
    String secondaryKey = genRandomKey(64);

    // Put <primaryKey, value1> into kv
    putKVandTestGet(primaryKey, value1);

    // Put <secondaryKey, value2> into kv
    putKVandTestGet(secondaryKey, value2);

    long ttl = GET_BACKOFF + GET_BACKOFF / 2;

    long startTs = session.getTimestamp().getVersion();
    long endTs = session.getTimestamp().getVersion();

    // prewriteString <primary key, value1>
    assertTrue(prewriteString(primaryKey, value3, startTs, primaryKey, ttl));

    // prewriteString <secondary key, value2>
    assertTrue(prewriteString(secondaryKey, value4, startTs, primaryKey, ttl));

    // non blocking read
    assertEquals(pointGet(primaryKey), value1);
    assertEquals(pointGet(secondaryKey), value2);

    // TTL expires
    Thread.sleep(ttl);

    // read old data & resolve lock
    assertEquals(pointGet(primaryKey), value1);
    assertEquals(pointGet(secondaryKey), value2);

    // commitString fail
    commitFail(primaryKey, startTs, endTs);
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

    String primaryKey = genRandomKey(64);
    String secondaryKey = genRandomKey(64);

    // Put <primaryKey, value1> into kv
    putKVandTestGet(primaryKey, value1);

    // Put <secondaryKey, value2> into kv
    putKVandTestGet(secondaryKey, value2);

    long ttl = GET_BACKOFF + GET_BACKOFF / 2;

    long startTs = session.getTimestamp().getVersion();

    // prewriteString <primary key, value1>
    assertTrue(prewriteString(primaryKey, value3, startTs, primaryKey, ttl));

    // prewriteString <secondary key, value2>
    assertTrue(prewriteString(secondaryKey, value4, startTs, primaryKey, ttl));

    // get new endTs
    long endTs = session.getTimestamp().getVersion();

    // non blocking read
    assertEquals(pointGet(primaryKey), value1);
    assertEquals(pointGet(secondaryKey), value2);

    try {
      // TTL not expires, but CommitTS expired, commit failed
      commitString(Collections.singletonList(primaryKey), startTs, endTs);
      fail();
    } catch (KeyException e) {
      TestCase.assertTrue(
          e.getMessage().startsWith("Key exception occurred and the reason is commit_ts_expired"));
    }
  }

  @Test
  public void NonBlockingReadCommitSuccess() throws InterruptedException {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isLockResolverClientV4()) {
      skipTestTiDBV4();
      return;
    }

    String primaryKey = genRandomKey(64);
    String secondaryKey = genRandomKey(64);

    // Put <primaryKey, value1> into kv
    putKVandTestGet(primaryKey, value1);

    // Put <secondaryKey, value2> into kv
    putKVandTestGet(secondaryKey, value2);

    long ttl = GET_BACKOFF + GET_BACKOFF / 2;

    long startTs = session.getTimestamp().getVersion();

    // prewriteString <primary key, value1>
    assertTrue(prewriteString(primaryKey, value3, startTs, primaryKey, ttl));

    // prewriteString <secondary key, value2>
    assertTrue(prewriteString(secondaryKey, value4, startTs, primaryKey, ttl));

    // non blocking read
    assertEquals(pointGet(primaryKey), value1);
    assertEquals(pointGet(secondaryKey), value2);

    // get new endTs
    long endTs = session.getTimestamp().getVersion();

    // commitString primary key
    assertTrue(commitString(Collections.singletonList(primaryKey), startTs, endTs));

    // get check primary key & secondary key
    assertEquals(pointGet(primaryKey), value3);
    assertEquals(pointGet(secondaryKey), value4);
  }

  @Test
  public void checkPrimaryTTL() throws InterruptedException {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isLockResolverClientV4()) {
      skipTestTiDBV4();
      return;
    }

    String primaryKey = genRandomKey(64);
    String secondaryKey = genRandomKey(64);

    // Put <primaryKey, value1> into kv
    putKVandTestGet(primaryKey, value1);

    // Put <secondaryKey, value2> into kv
    putKVandTestGet(secondaryKey, value2);

    long primaryTTL = GET_BACKOFF + GET_BACKOFF / 2;
    long secondaryTTL = DEFAULT_TTL;

    long startTs = session.getTimestamp().getVersion();

    // prewriteString <primary key, value1>
    assertTrue(prewriteString(primaryKey, value3, startTs, primaryKey, primaryTTL));

    // prewriteString <secondary key, value2>
    assertTrue(prewriteString(secondaryKey, value4, startTs, primaryKey, secondaryTTL));

    // secondary ttl expired, but primary not
    Thread.sleep(secondaryTTL);

    // non blocking read
    assertEquals(pointGet(primaryKey), value1);
    assertEquals(pointGet(secondaryKey), value2);

    // get new endTs
    long endTs = session.getTimestamp().getVersion();

    // commitString primary key
    assertTrue(commitString(Collections.singletonList(primaryKey), startTs, endTs));

    // get check primary key & secondary key
    assertEquals(pointGet(primaryKey), value3);
    assertEquals(pointGet(secondaryKey), value4);
  }
}
