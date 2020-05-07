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
import com.pingcap.tikv.exception.GrpcException;
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
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.Mutation;
import org.tikv.kvproto.Kvrpcpb.Op;

abstract class LockResolverTest {
  private Kvrpcpb.IsolationLevel isolationLevel;

  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  TiSession session;
  static final int DEFAULT_TTL = 10;
  RegionStoreClient.RegionStoreClientBuilder builder;
  boolean init;
  private static final String DEFAULT_PD_ADDR = "127.0.0.1:2379";
  private static final long LARGE_LOCK_TTL = BackOffer.GET_MAX_BACKOFF + 2 * 1000;

  static final int GET_BACKOFF = 5 * 1000;
  static final int CHECK_TTL_BACKOFF = 1000;

  private String getPdAddr() {
    String tmp = System.getenv("pdAddr");
    if (tmp != null && !tmp.equals("")) {
      return tmp;
    }

    tmp = System.getProperty("pdAddr");
    if (tmp != null && !tmp.equals("")) {
      return tmp;
    }

    return DEFAULT_PD_ADDR;
  }

  LockResolverTest(Kvrpcpb.IsolationLevel isolationLevel) {
    this.isolationLevel = isolationLevel;
  }

  @Before
  public void setUp() {
    TiConfiguration conf = TiConfiguration.createDefault(getPdAddr());
    conf.setIsolationLevel(isolationLevel);
    try {
      session = TiSession.getInstance(conf);
      this.builder = session.getRegionStoreClientBuilder();
      init = true;
    } catch (Exception e) {
      init = false;
      fail("TiDB cluster may not be present");
    }
  }

  void putKV(String key, String value, long startTS, long commitTS) {
    Mutation m =
        Mutation.newBuilder()
            .setKey(ByteString.copyFromUtf8(key))
            .setOp(Op.Put)
            .setValue(ByteString.copyFromUtf8(value))
            .build();

    boolean res = prewriteString(Collections.singletonList(m), startTS, key, DEFAULT_TTL);
    assertTrue(res);
    res = commitString(Collections.singletonList(key), startTS, commitTS);
    assertTrue(res);
  }

  boolean prewriteString(String key, String value, long startTS, String primaryKey, long ttl) {
    Mutation m =
        Mutation.newBuilder()
            .setKey(ByteString.copyFromUtf8(key))
            .setOp(Op.Put)
            .setValue(ByteString.copyFromUtf8(value))
            .build();

    return prewriteString(Collections.singletonList(m), startTS, primaryKey, ttl);
  }

  boolean prewriteString(List<Mutation> mutations, long startTS, String primary, long ttl) {
    return prewrite(mutations, startTS, ByteString.copyFromUtf8(primary), ttl);
  }

  boolean prewrite(List<Mutation> mutations, long startTS, ByteString primary, long ttl) {
    if (mutations.size() == 0) return true;
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(1000);

    for (Mutation m : mutations) {
      while (true) {
        try {
          TiRegion region = session.getRegionManager().getRegionByKey(m.getKey());
          RegionStoreClient client = builder.build(region);
          client.prewrite(backOffer, primary, Collections.singletonList(m), startTS, ttl);
          break;
        } catch (RegionException e) {
          backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        }
      }
    }
    return true;
  }

  boolean commitString(List<String> keys, long startTS, long commitTS) {
    return commit(
        keys.stream().map(ByteString::copyFromUtf8).collect(Collectors.toList()),
        startTS,
        commitTS);
  }

  boolean commit(List<ByteString> keys, long startTS, long commitTS) {
    if (keys.size() == 0) return true;
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(1000);

    for (ByteString byteStringK : keys) {
      while (true) {
        try {
          TiRegion tiRegion = session.getRegionManager().getRegionByKey(byteStringK);
          RegionStoreClient client = builder.build(tiRegion);
          client.commit(backOffer, Collections.singletonList(byteStringK), startTS, commitTS);
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
    return lockKey(
        key, value, primaryKey, primaryValue, commitPrimary, startTs, commitTS, DEFAULT_TTL);
  }

  boolean lockKey(
      String key,
      String value,
      String primaryKey,
      String primaryValue,
      boolean commitPrimary,
      long startTs,
      long commitTS,
      long ttl) {
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
    if (!prewriteString(mutations, startTs, primaryKey, ttl)) return false;

    if (commitPrimary) {
      if (!key.equals(primaryKey)) {
        return commitString(Arrays.asList(primaryKey, key), startTs, commitTS);
      } else {
        return commitString(Collections.singletonList(primaryKey), startTs, commitTS);
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
    assertTrue(
        lockKey(
            "d",
            "dd",
            "z2",
            "z2",
            false,
            startTs.getVersion(),
            endTs.getVersion(),
            LARGE_LOCK_TTL));
  }

  void skipTestInit() {
    logger.warn("Test skipped due to failure in initializing pd client.");
  }

  void skipTestV3() {
    logger.warn("Test skipped due to version of TiKV is to low.");
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
      } catch (GrpcException e) {
        assertEquals(e.getMessage(), "retry is exhausted.");
      }
    }
  }

  String genRandomKey(int strLength) {
    Random rnd = ThreadLocalRandom.current();
    StringBuilder ret = new StringBuilder();
    for (int i = 0; i < strLength; i++) {
      boolean isChar = (rnd.nextInt(2) % 2 == 0);
      if (isChar) {
        int choice = rnd.nextInt(2) % 2 == 0 ? 65 : 97;
        ret.append((char) (choice + rnd.nextInt(26)));
      } else {
        ret.append(Integer.toString(rnd.nextInt(10)));
      }
    }
    return ret.toString();
  }

  RegionStoreClient getRegionStoreClient(String key) {
    TiRegion tiRegion = session.getRegionManager().getRegionByKey(ByteString.copyFromUtf8(key));
    return builder.build(tiRegion);
  }

  void checkTTLNotExpired(String key) {
    try {
      RegionStoreClient client = getRegionStoreClient(key);
      BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(CHECK_TTL_BACKOFF);
      // In SI mode, a lock <key, value2> is read. Try resolve it, but failed, cause TTL not
      // expires.
      client.get(backOffer, ByteString.copyFromUtf8(key), session.getTimestamp().getVersion());
      fail();
    } catch (GrpcException e) {
      assertEquals(e.getMessage(), "retry is exhausted.");
    }
  }

  String pointGet(String key) {
    BackOffer backOffer2 = ConcreteBackOffer.newCustomBackOff(GET_BACKOFF);
    RegionStoreClient client = getRegionStoreClient(key);
    return client
        .get(backOffer2, ByteString.copyFromUtf8(key), session.getTimestamp().getVersion())
        .toStringUtf8();
  }

  void commitFail(String key, long startTs, long endTs) {
    try {
      // Trying to continue the commitString phase of <key, value2> will fail because
      // TxnLockNotFound
      commitString(Collections.singletonList(key), startTs, endTs);
      fail();
    } catch (KeyException e) {
      assertFalse(e.getKeyError().getRetryable().isEmpty());
    }
  }

  void putKVandTestGet(String key, String value) {
    RegionStoreClient client = getRegionStoreClient(key);

    TiTimestamp startTs = session.getTimestamp();
    TiTimestamp endTs = session.getTimestamp();
    putKV(key, value, startTs.getVersion(), endTs.getVersion());

    BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
    ByteString v =
        client.get(backOffer, ByteString.copyFromUtf8(key), session.getTimestamp().getVersion());
    assertEquals(v.toStringUtf8(), value);
  }

  boolean isV3() {
    return getRegionStoreClient("").lockResolverClient.getVersion().equals("V3");
  }
}
