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
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffFunction;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.tikv.kvproto.Kvrpcpb.*;

public abstract class LockResolverTest {
  private final Logger logger = Logger.getLogger(this.getClass());
  TiSession session;
  private static final int DefaultTTL = 10;
  RegionStoreClient.RegionStoreClientBuilder builder;
  boolean init;

  @Before
  public abstract void setUp();

  void putKV(String key, String value, long startTS, long commitTS) {
    Mutation m =
        Mutation.newBuilder()
            .setKey(ByteString.copyFromUtf8(key))
            .setOp(Op.Put)
            .setValue(ByteString.copyFromUtf8(value))
            .build();

    boolean res = prewrite(Collections.singletonList(m), startTS, m);
    assertTrue(res);
    res = commit(Collections.singletonList(ByteString.copyFromUtf8(key)), startTS, commitTS);
    assertTrue(res);
  }

  boolean prewrite(List<Mutation> mutations, long startTS, Mutation primary) {
    if (mutations.size() == 0) return true;
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(1000);

    for (Mutation m : mutations) {
      while (true) {
        try {
          TiRegion region = session.getRegionManager().getRegionByKey(m.getKey());
          RegionStoreClient client = builder.build(region);
          client.prewrite(
              backOffer, primary.getKey(), Collections.singletonList(m), startTS, DefaultTTL);
          break;
        } catch (RegionException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        }
      }
    }
    return true;
  }

  boolean commit(List<ByteString> keys, long startTS, long commitTS) {
    if (keys.size() == 0) return true;
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(1000);

    for (ByteString k : keys) {
      while (true) {
        try {
          TiRegion tiRegion = session.getRegionManager().getRegionByKey(k);
          RegionStoreClient client = builder.build(tiRegion);
          client.commit(backOffer, Collections.singletonList(k), startTS, commitTS);
          break;
        } catch (RegionException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        }
      }
    }
    return true;
  }

  boolean lockKey(
      String key,
      String value,
      String primaryKey,
      String primaryValue,
      boolean commitPrimary,
      long startTs,
      long commitTS) {
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newBuilder()
            .setKey(ByteString.copyFromUtf8(primaryKey))
            .setValue(ByteString.copyFromUtf8(primaryValue))
            .setOp(Op.Put)
            .build());
    if (!key.equals(primaryKey)) {
      mutations.add(
          Mutation.newBuilder()
              .setKey(ByteString.copyFromUtf8(key))
              .setValue(ByteString.copyFromUtf8(value))
              .setOp(Op.Put)
              .build());
    }
    if (!prewrite(mutations, startTs, mutations.get(0))) return false;

    if (commitPrimary) {
      if (!key.equals(primaryKey)) {
        return commit(
            Arrays.asList(ByteString.copyFromUtf8(primaryKey), ByteString.copyFromUtf8(key)),
            startTs,
            commitTS);
      } else {
        return commit(
            Collections.singletonList(ByteString.copyFromUtf8(primaryKey)), startTs, commitTS);
      }
    }

    return true;
  }

  void putAlphabet() {
    for (int i = 0; i < 26; i++) {
      long startTs = session.getTimestamp().getVersion();
      long endTs = session.getTimestamp().getVersion();
      while (startTs == endTs) {
        endTs = session.getTimestamp().getVersion();
      }
      putKV(String.valueOf((char) ('a' + i)), String.valueOf((char) ('a' + i)), startTs, endTs);
    }
    versionTest();
  }

  void prepareAlphabetLocks() {
    TiTimestamp startTs = session.getTimestamp();
    TiTimestamp endTs = session.getTimestamp();
    while (startTs == endTs) {
      endTs = session.getTimestamp();
    }
    putKV("c", "cc", startTs.getVersion(), endTs.getVersion());
    startTs = session.getTimestamp();
    endTs = session.getTimestamp();
    while (startTs == endTs) {
      endTs = session.getTimestamp();
    }

    assertTrue(lockKey("c", "c", "z1", "z1", true, startTs.getVersion(), endTs.getVersion()));
    startTs = session.getTimestamp();
    endTs = session.getTimestamp();
    while (startTs == endTs) {
      endTs = session.getTimestamp();
    }
    assertTrue(lockKey("d", "dd", "z2", "z2", false, startTs.getVersion(), endTs.getVersion()));
  }

  void skipTest() {
    logger.warn("Test skipped due to failure in initializing pd client.");
  }

  void versionTest() {
    versionTest(false);
  }

  void versionTest(boolean hasLock) {
    for (int i = 0; i < 26; i++) {
      ByteString key = ByteString.copyFromUtf8(String.valueOf((char) ('a' + i)));
      TiRegion tiRegion = session.getRegionManager().getRegionByKey(key);
      RegionStoreClient client = builder.build(tiRegion);
      BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
      try {
        ByteString v = client.get(backOffer, key, session.getTimestamp().getVersion());
        if (hasLock && i == 3) {
          // key "d" should be locked
          fail();
        } else {
          assertEquals(String.valueOf((char) ('a' + i)), v.toStringUtf8());
        }
      } catch (KeyException e) {
        assertEquals(ByteString.copyFromUtf8("d"), key);
        LockInfo lock = e.getKeyError().getLocked();
        assertEquals(key, lock.getKey());
        assertEquals(ByteString.copyFromUtf8("z2"), lock.getPrimaryLock());
      }
    }
  }
}
