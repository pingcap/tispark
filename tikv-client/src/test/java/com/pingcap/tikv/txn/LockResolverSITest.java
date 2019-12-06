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

import static junit.framework.TestCase.*;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.tikv.kvproto.Kvrpcpb.*;

public class LockResolverSITest extends LockResolverTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Before
  public void setUp() {
    TiConfiguration conf = TiConfiguration.createDefault(pdAddr);
    conf.setIsolationLevel(IsolationLevel.SI);
    try {
      session = TiSession.getInstance(conf);
      this.builder = session.getRegionStoreClientBuilder();
      init = true;
    } catch (Exception e) {
      init = false;
      fail("TiDB cluster may not be present");
    }
  }

  @Test
  public void getSITest() {
    if (!init) {
      skipTest();
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
      skipTest();
      return;
    }
    for (int i = 0; i < 26; i++) {
      String k = String.valueOf((char) ('a' + i));
      TiTimestamp startTs = session.getTimestamp();
      TiTimestamp endTs = session.getTimestamp();
      assertTrue(lockKey(k, k, k, k, false, startTs.getVersion(), endTs.getVersion()));
    }

    List<Mutation> mutations = new ArrayList<>();
    List<ByteString> keys = new ArrayList<>();
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
      keys.add(ByteString.copyFromUtf8(k));
    }

    TiTimestamp startTs = session.getTimestamp();
    TiTimestamp endTs = session.getTimestamp();

    boolean res = prewrite(mutations, startTs.getVersion(), mutations.get(0));
    assertTrue(res);
    res = commit(keys, startTs.getVersion(), endTs.getVersion());
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
  public void txnStatusTest() {
    if (!init) {
      skipTest();
      return;
    }
    TiTimestamp startTs = session.getTimestamp();
    TiTimestamp endTs = session.getTimestamp();

    putKV("a", "a", startTs.getVersion(), endTs.getVersion());
    TiRegion tiRegion = session.getRegionManager().getRegionByKey(ByteString.copyFromUtf8("a"));
    RegionStoreClient client = builder.build(tiRegion);
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(BackOffer.CLEANUP_MAX_BACKOFF);
    long status =
        client.lockResolverClient.getTxnStatus(
            backOffer, startTs.getVersion(), ByteString.copyFromUtf8("a"));
    assertEquals(status, endTs.getVersion());

    startTs = session.getTimestamp();
    endTs = session.getTimestamp();

    assertTrue(lockKey("a", "a", "a", "a", true, startTs.getVersion(), endTs.getVersion()));
    tiRegion = session.getRegionManager().getRegionByKey(ByteString.copyFromUtf8("a"));
    client = builder.build(tiRegion);
    status =
        client.lockResolverClient.getTxnStatus(
            backOffer, startTs.getVersion(), ByteString.copyFromUtf8("a"));
    assertEquals(status, endTs.getVersion());

    startTs = session.getTimestamp();
    endTs = session.getTimestamp();

    assertTrue(lockKey("a", "a", "a", "a", false, startTs.getVersion(), endTs.getVersion()));
    tiRegion = session.getRegionManager().getRegionByKey(ByteString.copyFromUtf8("a"));
    client = builder.build(tiRegion);
    status =
        client.lockResolverClient.getTxnStatus(
            backOffer, startTs.getVersion(), ByteString.copyFromUtf8("a"));
    assertNotSame(status, endTs.getVersion());
  }

  @Test
  public void SITest() {
    if (!init) {
      skipTest();
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

    try {
      BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
      // In SI mode, a lock <a, aa> is read. Try resolve it if expires TTL.
      client.get(backOffer, ByteString.copyFromUtf8("a"), session.getTimestamp().getVersion());
      fail();
    } catch (KeyException e) {
      assertEquals(ByteString.copyFromUtf8("a"), e.getKeyError().getLocked().getKey());
    }

    {
      BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
      // With TTL set to 10, after 10 milliseconds <a, aa> is resolved.
      // We should be able to read <a, a> instead.
      ByteString v =
          client.get(backOffer, ByteString.copyFromUtf8("a"), session.getTimestamp().getVersion());
      assertEquals(v.toStringUtf8(), String.valueOf('a'));
    }

    try {
      // Trying to continue the commit phase of <a, aa> will fail because TxnLockNotFound
      commit(
          Collections.singletonList(ByteString.copyFromUtf8("a")),
          startTs.getVersion(),
          endTs.getVersion());
      fail();
    } catch (KeyException e) {
      assertFalse(e.getKeyError().getRetryable().isEmpty());
    }
  }
}
