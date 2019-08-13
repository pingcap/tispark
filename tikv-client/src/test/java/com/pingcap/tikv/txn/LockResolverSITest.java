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
    TiConfiguration conf = TiConfiguration.createDefault("127.0.0.1:2379");
    conf.setIsolationLevel(IsolationLevel.SI);
    TiSession.clearCache();
    try {
      session = TiSession.getInstance(conf);
      pdClient = session.getPDClient();
      this.builder = session.getRegionStoreClientBuilder();
      init = true;
    } catch (Exception e) {
      logger.warn("TiDB cluster may not be present");
      init = false;
    }
  }

  @Test
  public void getSITest() {
    if (!init) {
      skipTest();
      return;
    }
    putAlphabet();
    prepareAlphabetLocks();

    versionTest();
  }

  @Test
  public void cleanLockTest() {
    if (!init) {
      skipTest();
      return;
    }
    for (int i = 0; i < 26; i++) {
      String k = String.valueOf((char) ('a' + i));
      TiTimestamp startTs = pdClient.getTimestamp(backOffer);
      TiTimestamp endTs = pdClient.getTimestamp(backOffer);
      lockKey(k, k, k, k, false, startTs.getVersion(), endTs.getVersion());
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

    TiTimestamp startTs = pdClient.getTimestamp(backOffer);
    TiTimestamp endTs = pdClient.getTimestamp(backOffer);

    boolean res = prewrite(mutations, startTs.getVersion(), mutations.get(0));
    assertTrue(res);
    res = commit(startTs.getVersion(), endTs.getVersion(), keys);
    assertTrue(res);

    for (int i = 0; i < 26; i++) {
      TiRegion tiRegion =
          session
              .getRegionManager()
              .getRegionByKey(ByteString.copyFromUtf8(String.valueOf((char) ('a' + i))));
      RegionStoreClient client = builder.build(tiRegion);
      ByteString v =
          client.get(
              backOffer,
              ByteString.copyFromUtf8(String.valueOf((char) ('a' + i))),
              pdClient.getTimestamp(backOffer).getVersion());
      assertEquals(v.toStringUtf8(), String.valueOf((char) ('a' + i + 1)));
    }
  }

  @Test
  public void txnStatusTest() {
    if (!init) {
      skipTest();
      return;
    }
    TiTimestamp startTs = pdClient.getTimestamp(backOffer);
    TiTimestamp endTs = pdClient.getTimestamp(backOffer);

    putKV("a", "a", startTs.getVersion(), endTs.getVersion());
    TiRegion tiRegion =
        session.getRegionManager().getRegionByKey(ByteString.copyFromUtf8(String.valueOf('a')));
    RegionStoreClient client = builder.build(tiRegion);
    long status =
        client.lockResolverClient.getTxnStatus(
            backOffer, startTs.getVersion(), ByteString.copyFromUtf8(String.valueOf('a')));
    assertEquals(status, endTs.getVersion());

    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);

    lockKey("a", "a", "a", "a", true, startTs.getVersion(), endTs.getVersion());
    tiRegion =
        session.getRegionManager().getRegionByKey(ByteString.copyFromUtf8(String.valueOf('a')));
    client = builder.build(tiRegion);
    status =
        client.lockResolverClient.getTxnStatus(
            backOffer, startTs.getVersion(), ByteString.copyFromUtf8(String.valueOf('a')));
    assertEquals(status, endTs.getVersion());

    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);

    lockKey("a", "a", "a", "a", false, startTs.getVersion(), endTs.getVersion());
    tiRegion =
        session.getRegionManager().getRegionByKey(ByteString.copyFromUtf8(String.valueOf('a')));
    client = builder.build(tiRegion);
    status =
        client.lockResolverClient.getTxnStatus(
            backOffer, startTs.getVersion(), ByteString.copyFromUtf8(String.valueOf('a')));
    assertNotSame(status, endTs.getVersion());
  }

  @Test
  public void SITest() {
    if (!init) {
      skipTest();
      return;
    }
    TiTimestamp startTs = pdClient.getTimestamp(backOffer);
    TiTimestamp endTs = pdClient.getTimestamp(backOffer);

    putKV("a", "a", startTs.getVersion(), endTs.getVersion());

    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);

    lockKey("a", "aa", "a", "aa", false, startTs.getVersion(), endTs.getVersion());

    TiRegion tiRegion =
        session.getRegionManager().getRegionByKey(ByteString.copyFromUtf8(String.valueOf('a')));
    RegionStoreClient client = builder.build(tiRegion);
    ByteString v =
        client.get(
            backOffer,
            ByteString.copyFromUtf8(String.valueOf('a')),
            pdClient.getTimestamp(backOffer).getVersion());
    assertEquals(v.toStringUtf8(), String.valueOf('a'));

    try {
      commit(
          startTs.getVersion(),
          endTs.getVersion(),
          Collections.singletonList(ByteString.copyFromUtf8("a")));
      fail();
    } catch (KeyException e) {
    }
  }
}
