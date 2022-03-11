/*
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
 */

package com.pingcap.tikv.txn;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;
import org.tikv.kvproto.Kvrpcpb.Mutation;
import org.tikv.kvproto.Kvrpcpb.Op;

public class LockResolverSITest extends LockResolverTest {
  public LockResolverSITest() {
    super(IsolationLevel.SI);
  }

  @Test
  public void getSITest() {
    if (!init) {
      skipTestInit();
      return;
    }

    session.getConf().setIsolationLevel(IsolationLevel.SI);
    putAlphabet();
    prepareAlphabetLocks();

    versionTest(true);
  }

  @Test
  public void cleanLockTest() {
    if (!init) {
      skipTestInit();
      return;
    }

    for (int i = 0; i < 26; i++) {
      String k = String.valueOf((char) ('a' + i));
      TiTimestamp startTs = session.getTimestamp();
      TiTimestamp endTs = session.getTimestamp();
      assertTrue(lockKey(k, k, k, k, false, startTs.getVersion(), endTs.getVersion()));
    }

    List<Mutation> mutations = new ArrayList<>();
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < 26; i++) {
      String k = String.valueOf((char) ('a' + i));
      String v = String.valueOf((char) ('a' + i + 1));
      Mutation m =
          Mutation.newBuilder()
              .setKey(ByteString.copyFromUtf8(k))
              .setOp(Op.Put)
              .setValue(ByteString.copyFromUtf8(v))
              .build();
      mutations.add(m);
      keys.add(k);
    }

    TiTimestamp startTs = session.getTimestamp();
    TiTimestamp endTs = session.getTimestamp();

    boolean res =
        prewriteString(
            mutations, startTs.getVersion(), mutations.get(0).getKey().toStringUtf8(), DEFAULT_TTL);
    assertTrue(res);
    res = commitString(keys, startTs.getVersion(), endTs.getVersion());
    assertTrue(res);

    for (int i = 0; i < 26; i++) {
      TiRegion tiRegion =
          session
              .getRegionManager()
              .getRegionByKey(ByteString.copyFromUtf8(String.valueOf((char) ('a' + i))));
      RegionStoreClient client = builder.build(tiRegion);
      BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
      ByteString v =
          client.get(
              backOffer,
              ByteString.copyFromUtf8(String.valueOf((char) ('a' + i))),
              session.getTimestamp().getVersion());
      assertEquals(v.toStringUtf8(), String.valueOf((char) ('a' + i + 1)));
    }
  }

  @Test
  public void SITestBlocking() {
    if (!init) {
      skipTestInit();
      return;
    }

    if (isLockResolverClientV4()) {
      logger.warn("Test skipped due to version of TiDB/TiKV should be 2.x or 3.x.");
      return;
    }

    TiTimestamp startTs = session.getTimestamp();
    TiTimestamp endTs = session.getTimestamp();

    // Put <a, a> into kv
    putKV("a", "a", startTs.getVersion(), endTs.getVersion());

    startTs = session.getTimestamp();
    endTs = session.getTimestamp();

    // Prewrite <a, aa> as primary without committing it
    assertTrue(lockKey("a", "aa", "a", "aa", false, startTs.getVersion(), endTs.getVersion()));

    TiRegion tiRegion = session.getRegionManager().getRegionByKey(ByteString.copyFromUtf8("a"));
    RegionStoreClient client = builder.build(tiRegion);

    {
      BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
      // With TTL set to 10, after 10 milliseconds <a, aa> is resolved.
      // We should be able to read <a, a> instead.
      ByteString v =
          client.get(backOffer, ByteString.copyFromUtf8("a"), session.getTimestamp().getVersion());
      assertEquals(v.toStringUtf8(), String.valueOf('a'));
    }

    try {
      // Trying to continue the commitString phase of <a, aa> will fail because TxnLockNotFound
      commitString(Collections.singletonList("a"), startTs.getVersion(), endTs.getVersion());
      fail();
    } catch (KeyException e) {
      assertFalse(e.getKeyError().getRetryable().isEmpty());
    }
  }

  @Test
  public void SITestNonBlocking() {
    if (!init) {
      skipTestInit();
      return;
    }

    if (!isLockResolverClientV4()) {
      skipTestTiDBV4();
      return;
    }

    TiTimestamp startTs = session.getTimestamp();
    TiTimestamp endTs = session.getTimestamp();

    // Put <a, a> into kv
    putKV("a", "a", startTs.getVersion(), endTs.getVersion());

    startTs = session.getTimestamp();
    endTs = session.getTimestamp();

    // Prewrite <a, aa> as primary without committing it
    assertTrue(
        lockKey(
            "a", "aa", "a", "aa", false, startTs.getVersion(), endTs.getVersion(), LARGE_LOCK_TTL));

    TiRegion tiRegion = session.getRegionManager().getRegionByKey(ByteString.copyFromUtf8("a"));
    RegionStoreClient client = builder.build(tiRegion);

    {
      BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
      Long callerTS = session.getTimestamp().getVersion();
      System.out.println("callerTS1= " + callerTS);
      ByteString v = client.get(backOffer, ByteString.copyFromUtf8("a"), callerTS);
      assertEquals(v.toStringUtf8(), String.valueOf('a'));
    }

    try {
      // Trying to continue the commitString phase of <a, aa> will fail because CommitTS <
      // MinCommitTS
      commitString(Collections.singletonList("a"), startTs.getVersion(), endTs.getVersion());
      fail();
    } catch (KeyException e) {
      assertTrue(
          e.getMessage().startsWith("Key exception occurred and the reason is commit_ts_expired"));
    }

    // Trying to continue the commitString phase of <a, aa> will success because CommitTS >
    // MinCommitTS
    endTs = session.getTimestamp();
    assertTrue(
        commitString(Collections.singletonList("a"), startTs.getVersion(), endTs.getVersion()));
  }
}
