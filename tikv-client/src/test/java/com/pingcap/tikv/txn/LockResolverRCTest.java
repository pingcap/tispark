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
import java.util.Collections;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.tikv.kvproto.Kvrpcpb.*;

public class LockResolverRCTest extends LockResolverTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Before
  public void setUp() {
    TiConfiguration conf = TiConfiguration.createDefault(pdAddr);
    conf.setIsolationLevel(IsolationLevel.RC);
    try {
      session = TiSession.getInstance(conf);
      this.builder = session.getRegionStoreClientBuilder();
      init = true;
    } catch (Exception e) {
      fail("TiDB cluster may not be present");
      init = false;
    }
  }

  @Test
  public void getRCTest() {
    if (!init) {
      skipTest();
      return;
    }
    session.getConf().setIsolationLevel(IsolationLevel.RC);
    putAlphabet();
    prepareAlphabetLocks();

    versionTest();
  }

  @Test
  public void RCTest() {
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

    {
      BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
      // In RC mode, lock will not be read. <a, a> is retrieved.
      ByteString v =
          client.get(backOffer, ByteString.copyFromUtf8("a"), session.getTimestamp().getVersion());
      assertEquals(v.toStringUtf8(), "a");
    }

    try {
      // After committing <a, aa>, we can read it.
      assertTrue(
          commit(
              Collections.singletonList(ByteString.copyFromUtf8("a")),
              startTs.getVersion(),
              endTs.getVersion()));
      BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
      ByteString v =
          client.get(backOffer, ByteString.copyFromUtf8("a"), session.getTimestamp().getVersion());
      assertEquals(v.toStringUtf8(), "aa");
    } catch (KeyException e) {
      fail();
    }
  }
}
