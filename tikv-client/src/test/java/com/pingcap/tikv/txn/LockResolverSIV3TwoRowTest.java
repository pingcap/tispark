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

import java.util.Collections;
import org.junit.Test;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;

public class LockResolverSIV3TwoRowTest extends LockResolverTest {
  private final String value1 = "v1";
  private final String value2 = "v2";
  private final String value3 = "v3";
  private final String value4 = "v4";

  public LockResolverSIV3TwoRowTest() {
    super(IsolationLevel.SI);
  }

  @Test
  public void prewriteCommitSuccessTest() {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isLockResolverClientV3()) {
      skipTestTiDBV3();
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
  public void TTLExpireCommitFail() throws InterruptedException {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isLockResolverClientV3()) {
      skipTestTiDBV3();
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

    // check ttl not expired
    checkTTLNotExpired(primaryKey);
    checkTTLNotExpired(secondaryKey);

    // TTL expires
    Thread.sleep(ttl);

    // read old data
    assertEquals(pointGet(primaryKey), value1);
    assertEquals(pointGet(secondaryKey), value2);

    // commitString fail
    commitFail(primaryKey, startTs, endTs);
  }

  @Test
  public void TTLExpireCommitSuccess() throws InterruptedException {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isLockResolverClientV3()) {
      skipTestTiDBV3();
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

    // check ttl not expired
    checkTTLNotExpired(primaryKey);
    checkTTLNotExpired(secondaryKey);

    // TTL expires
    Thread.sleep(ttl);

    // commitString primary key
    assertTrue(commitString(Collections.singletonList(primaryKey), startTs, endTs));

    // get check primary key & secondary key
    assertEquals(pointGet(primaryKey), value3);
    assertEquals(pointGet(secondaryKey), value4);
  }

  @Test
  public void TTLNotExpireCommitSuccess() throws InterruptedException {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isLockResolverClientV3()) {
      skipTestTiDBV3();
      return;
    }

    String primaryKey = genRandomKey(64);
    String secondaryKey = genRandomKey(64);

    long ttl = GET_BACKOFF + GET_BACKOFF / 2;

    long startTs = session.getTimestamp().getVersion();
    long endTs = session.getTimestamp().getVersion();

    // prewriteString <primary key, value1>
    assertTrue(prewriteString(primaryKey, value1, startTs, primaryKey, ttl));

    // prewriteString <secondary key, value2>
    assertTrue(prewriteString(secondaryKey, value2, startTs, primaryKey, ttl));

    // check ttl not expired
    checkTTLNotExpired(primaryKey);
    checkTTLNotExpired(secondaryKey);

    // commitString primary key
    assertTrue(commitString(Collections.singletonList(primaryKey), startTs, endTs));

    // get check primary key & secondary key
    assertEquals(pointGet(primaryKey), value1);

    // secondary key ttl not expired
    checkTTLNotExpired(secondaryKey);

    // get secondary key
    Thread.sleep(ttl);
    assertEquals(pointGet(secondaryKey), value2);
  }

  @Test
  public void checkPrimaryTTL() throws InterruptedException {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isLockResolverClientV3()) {
      skipTestTiDBV3();
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

    // check ttl not expired
    checkTTLNotExpired(primaryKey);
    checkTTLNotExpired(secondaryKey);

    // secondary ttl expired & primary expired
    Thread.sleep(primaryTTL);

    // get check primary key & secondary key
    assertEquals(pointGet(primaryKey), value1);
    assertEquals(pointGet(secondaryKey), value2);
  }
}
