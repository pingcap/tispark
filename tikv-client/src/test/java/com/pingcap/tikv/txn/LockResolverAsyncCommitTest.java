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

import com.google.protobuf.ByteString;
import com.pingcap.tikv.Version;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;

public class LockResolverAsyncCommitTest extends LockResolverTest {

  private final int ASYNC_COMMIT_TTL = 3000;

  private final String oldValue = "o";
  private final String primaryKeyValue = "v";
  private final String[] secondaryKeyValueList = {"v0", "v1", "v2", "v3", "v4", "v5", "6", "v7"};
  private final int secondarySize = secondaryKeyValueList.length;

  public LockResolverAsyncCommitTest() {
    super(IsolationLevel.SI);
  }

  @Test
  public void prewriteCommitTest() {
    if (!check()) {
      return;
    }

    String primaryKey = genRandomKey(64);
    List<String> secondaryKeyList = randomSecondaryKeyList();

    // put
    putAll(primaryKey, secondaryKeyList);

    // prewrite keys
    long startTs = session.getTimestamp().getVersion();
    prewrite(startTs, primaryKey, secondaryKeyList);

    // commitString primary key
    long endTs = session.getTimestamp().getVersion();
    assertTrue(commitString(Collections.singletonList(primaryKey), startTs, endTs));

    // commitString secondary key
    for (int i = 0; i < secondarySize; i++) {
      assertTrue(commitString(Collections.singletonList(secondaryKeyList.get(i)), startTs, endTs));
    }

    // get check primary key & secondary key
    assertEquals(pointGet(primaryKey), primaryKeyValue);
    for (int i = 0; i < secondarySize; i++) {
      assertEquals(pointGet(secondaryKeyList.get(i)), secondaryKeyValueList[i]);
    }
  }

  @Test
  public void prewriteWithoutCommitSecondaryTest() {
    if (!check()) {
      return;
    }

    String primaryKey = genRandomKey(64);
    List<String> secondaryKeyList = randomSecondaryKeyList();

    // put
    putAll(primaryKey, secondaryKeyList);

    // prewrite keys
    long startTs = session.getTimestamp().getVersion();
    prewrite(startTs, primaryKey, secondaryKeyList);

    // commitString primary key
    long endTs = session.getTimestamp().getVersion();
    assertTrue(commitString(Collections.singletonList(primaryKey), startTs, endTs));

    for (int i = 0; i < secondarySize; i++) {
      if (i % 2 == 0) {
        // skip commitString secondary key
      } else {
        // commitString secondary key
        assertTrue(
            commitString(Collections.singletonList(secondaryKeyList.get(i)), startTs, endTs));
      }
    }

    // get check primary key & secondary key
    for (int i = 0; i < secondarySize; i++) {
      assertEquals(pointGet(secondaryKeyList.get(i)), secondaryKeyValueList[i]);
    }
    assertEquals(pointGet(primaryKey), primaryKeyValue);
  }

  @Test
  public void prewriteWithoutCommitPrimaryTest() {
    if (!check()) {
      return;
    }

    String primaryKey = genRandomKey(64);
    List<String> secondaryKeyList = randomSecondaryKeyList();

    // put
    putAll(primaryKey, secondaryKeyList);

    // prewrite keys
    long startTs = session.getTimestamp().getVersion();
    prewrite(startTs, primaryKey, secondaryKeyList);

    long endTs = session.getTimestamp().getVersion();

    // skip commitString primary key

    // commitString secondary key
    for (int i = 0; i < secondarySize; i++) {
      assertTrue(commitString(Collections.singletonList(secondaryKeyList.get(i)), startTs, endTs));
    }

    // get check primary key & secondary key
    for (int i = 0; i < secondarySize; i++) {
      assertEquals(pointGet(secondaryKeyList.get(i)), secondaryKeyValueList[i]);
    }
    assertEquals(pointGet(primaryKey), primaryKeyValue);
  }

  @Test
  public void prewriteWithCommitSomeSecondaryTest() {
    if (!check()) {
      return;
    }

    String primaryKey = genRandomKey(64);
    List<String> secondaryKeyList = randomSecondaryKeyList();

    // put
    putAll(primaryKey, secondaryKeyList);

    // prewrite keys
    long startTs = session.getTimestamp().getVersion();
    prewrite(startTs, primaryKey, secondaryKeyList);

    long endTs = session.getTimestamp().getVersion();

    // skip commitString primary key

    for (int i = 0; i < secondarySize; i++) {
      if (i % 2 == 0) {
        // commitString secondary key
        assertTrue(
            commitString(Collections.singletonList(secondaryKeyList.get(i)), startTs, endTs));
      } else {
        // skip commitString secondary key
      }
    }

    // get check primary key & secondary key
    for (int i = 0; i < secondarySize; i++) {
      assertEquals(pointGet(secondaryKeyList.get(i)), secondaryKeyValueList[i]);
    }
    assertEquals(pointGet(primaryKey), primaryKeyValue);
  }

  @Test
  public void prewriteWithoutCommitAllTest() {
    if (!check()) {
      return;
    }

    String primaryKey = genRandomKey(64);
    List<String> secondaryKeyList = randomSecondaryKeyList();

    // put
    putAll(primaryKey, secondaryKeyList);

    // prewrite keys
    long startTs = session.getTimestamp().getVersion();
    prewrite(startTs, primaryKey, secondaryKeyList);

    // skip commitString primary key

    // skip commitString secondary key

    // get check primary key & secondary key
    assertEquals(pointGet(primaryKey), primaryKeyValue);
    for (int i = 0; i < secondarySize; i++) {
      assertEquals(pointGet(secondaryKeyList.get(i)), secondaryKeyValueList[i]);
    }
  }

  @Test
  public void prewriteWtihoutSecondaryKeyTest() {
    if (!check()) {
      return;
    }

    String primaryKey = genRandomKey(64);
    List<String> secondaryKeyList = randomSecondaryKeyList();

    List<ByteString> secondaries =
        secondaryKeyList.stream().map(ByteString::copyFromUtf8).collect(Collectors.toList());

    // put
    putAll(primaryKey, secondaryKeyList);

    // prewriteString <primary key, value1, secondaries>
    long startTs = session.getTimestamp().getVersion();
    Assert.assertTrue(
        prewriteStringUsingAsyncCommit(
            primaryKey, primaryKeyValue, startTs, primaryKey, ASYNC_COMMIT_TTL, secondaries));

    for (int i = 0; i < secondaryKeyList.size(); i++) {
      if (i == secondarySize - 1) {
        // skip prewriteString secondary key
      } else {
        // prewriteString secondary key
        Assert.assertTrue(
            prewriteStringUsingAsyncCommit(
                secondaryKeyList.get(i),
                secondaryKeyValueList[i],
                startTs,
                primaryKey,
                ASYNC_COMMIT_TTL,
                null));
      }
    }

    // skip commitString primary key

    // skip commitString secondary key

    // get check primary key & secondary key
    for (int i = 0; i < secondarySize; i++) {
      assertEquals(pointGet(secondaryKeyList.get(i)), oldValue);
    }
    assertEquals(pointGet(primaryKey), oldValue);
  }

  @Test
  public void prewriteWtihoutPrimaryKeyTest() {
    if (!check()) {
      return;
    }

    String primaryKey = genRandomKey(64);
    List<String> secondaryKeyList = randomSecondaryKeyList();

    // put
    putAll(primaryKey, secondaryKeyList);

    // skip prewriteString <primary key, value1, secondaries>

    // prewriteString secondary key
    long startTs = session.getTimestamp().getVersion();
    for (int i = 0; i < secondaryKeyList.size(); i++) {
      Assert.assertTrue(
          prewriteStringUsingAsyncCommit(
              secondaryKeyList.get(i),
              secondaryKeyValueList[i],
              startTs,
              primaryKey,
              ASYNC_COMMIT_TTL,
              null));
    }

    // skip commitString primary key

    // skip commitString secondary key

    // get check primary key & secondary key
    for (int i = 0; i < secondarySize; i++) {
      assertEquals(pointGet(secondaryKeyList.get(i)), oldValue);
    }
    assertEquals(pointGet(primaryKey), oldValue);
  }

  @Test
  public void ttlExpiredTest() throws InterruptedException {
    if (!check()) {
      return;
    }

    String primaryKey = genRandomKey(64);
    List<String> secondaryKeyList = randomSecondaryKeyList();

    // put
    putAll(primaryKey, secondaryKeyList);

    // prewrite keys
    long startTs = session.getTimestamp().getVersion();
    prewrite(startTs, primaryKey, secondaryKeyList);

    // sleep
    Thread.sleep(ASYNC_COMMIT_TTL);

    // skip commitString primary key

    // skip commitString secondary key

    // get check primary key & secondary key
    assertEquals(pointGet(primaryKey), primaryKeyValue);
    for (int i = 0; i < secondarySize; i++) {
      assertEquals(pointGet(secondaryKeyList.get(i)), secondaryKeyValueList[i]);
    }
  }

  private boolean check() {
    if (!init) {
      skipTestInit();
      return false;
    }

    if (!supportAsyncCommit()) {
      logger.warn("Test skipped due to version of TiDB/TiKV should be " + Version.ASYNC_COMMIT);
      return false;
    }

    return true;
  }

  private List<String> randomSecondaryKeyList() {
    List<String> secondaryKeyList = new ArrayList<>();
    for (int i = 0; i < secondarySize; i++) {
      if (i % 3 == 0) {
        secondaryKeyList.add("a-" + i + "-" + genRandomKey(64));
      } else if (i % 3 == 1) {
        secondaryKeyList.add("j-" + i + "-" + genRandomKey(64));
      } else {
        secondaryKeyList.add("z-" + i + "-" + genRandomKey(64));
      }
    }
    return secondaryKeyList;
  }

  private void putAll(String primaryKey, List<String> secondaryKeyList) {
    putKV(primaryKey, oldValue);
    for (String secondaryKey : secondaryKeyList) {
      putKV(secondaryKey, oldValue);
    }
  }

  private void prewrite(long startTs, String primaryKey, List<String> secondaryKeyList) {
    List<ByteString> secondaries =
        secondaryKeyList.stream().map(ByteString::copyFromUtf8).collect(Collectors.toList());

    // prewriteString <primary key, value1, secondaries>
    Assert.assertTrue(
        prewriteStringUsingAsyncCommit(
            primaryKey, primaryKeyValue, startTs, primaryKey, ASYNC_COMMIT_TTL, secondaries));

    // prewriteString secondaryKeys
    for (int i = 0; i < secondaryKeyList.size(); i++) {
      Assert.assertTrue(
          prewriteStringUsingAsyncCommit(
              secondaryKeyList.get(i),
              secondaryKeyValueList[i],
              startTs,
              primaryKey,
              ASYNC_COMMIT_TTL,
              null));
    }
  }
}
