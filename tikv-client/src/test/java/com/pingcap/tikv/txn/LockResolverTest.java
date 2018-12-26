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

import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoTxnLock;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNotSame;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.PDClient;
import com.pingcap.tikv.ReadOnlyPDClient;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.kvproto.Kvrpcpb.CommitRequest;
import com.pingcap.tikv.kvproto.Kvrpcpb.CommitResponse;
import com.pingcap.tikv.kvproto.Kvrpcpb.IsolationLevel;
import com.pingcap.tikv.kvproto.Kvrpcpb.KeyError;
import com.pingcap.tikv.kvproto.Kvrpcpb.Mutation;
import com.pingcap.tikv.kvproto.Kvrpcpb.Op;
import com.pingcap.tikv.kvproto.Kvrpcpb.PrewriteRequest;
import com.pingcap.tikv.kvproto.Kvrpcpb.PrewriteResponse;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.kvproto.TikvGrpc;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.operation.KVErrorHandler;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class LockResolverTest {
  private final Logger logger = Logger.getLogger(this.getClass());
  private TiSession session;
  private static final int DefaultTTL = 10;
  private BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(1000);
  private ReadOnlyPDClient pdClient;

  private void putKV(String key, String value, long startTS, long commitTS) {
    Mutation m =
        Mutation.newBuilder()
            .setKey(ByteString.copyFromUtf8(key))
            .setOp(Op.Put)
            .setValue(ByteString.copyFromUtf8(value))
            .build();

    boolean res = prewrite(Collections.singletonList(m), startTS, m);
    assertTrue(res);
    res = commit(startTS, commitTS, Collections.singletonList(ByteString.copyFromUtf8(key)));
    assertTrue(res);
  }

  private boolean prewrite(List<Mutation> mutations, long startTS, Mutation primary) {
    if (mutations.size() == 0) return true;

    for (Mutation m : mutations) {
      Pair<TiRegion, Store> pair = session.getRegionManager().getRegionStorePairByKey(m.getKey());

      RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, session);

      Supplier<PrewriteRequest> factory =
          () ->
              PrewriteRequest.newBuilder()
                  .addAllMutations(Collections.singletonList(m))
                  .setPrimaryLock(primary.getKey())
                  .setStartVersion(startTS)
                  .setLockTtl(DefaultTTL)
                  .setContext(pair.first.getContext())
                  .build();

      KVErrorHandler<PrewriteResponse> handler =
          new KVErrorHandler<>(
              session.getRegionManager(),
              client,
              pair.first,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      PrewriteResponse resp =
          client.callWithRetry(backOffer, TikvGrpc.METHOD_KV_PREWRITE, factory, handler);

      if (resp.hasRegionError()) {
        throw new RegionException(resp.getRegionError());
      }

      if (resp.getErrorsCount() == 0) {
        continue;
      }

      List<Lock> locks = new ArrayList<>();
      for (KeyError err : resp.getErrorsList()) {
        if (err.hasLocked()) {
          Lock lock = new Lock(err.getLocked());
          locks.add(lock);
        } else {
          throw new KeyException(err);
        }
      }

      LockResolverClient resolver = null;
      try {
        Field field = RegionStoreClient.class.getDeclaredField("lockResolverClient");
        assert (field != null);
        field.setAccessible(true);
        resolver = (LockResolverClient) (field.get(client));
      } catch (Exception e) {
        fail();
      }

      assertNotNull(resolver);

      if (!resolver.resolveLocks(backOffer, locks)) {
        backOffer.doBackOff(BoTxnLock, new KeyException(resp.getErrorsList().get(0)));
      }

      prewrite(Collections.singletonList(m), startTS, primary);
    }

    return true;
  }

  private boolean lockKey(
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
            startTs,
            commitTS,
            Arrays.asList(ByteString.copyFromUtf8(primaryKey), ByteString.copyFromUtf8(key)));
      } else {
        return commit(
            startTs, commitTS, Collections.singletonList(ByteString.copyFromUtf8(primaryKey)));
      }
    }

    return true;
  }

  private boolean commit(long startTS, long commitTS, List<ByteString> keys) {
    if (keys.size() == 0) return true;

    for (ByteString k : keys) {
      Pair<TiRegion, Store> pair = session.getRegionManager().getRegionStorePairByKey(k);

      RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, session);
      Supplier<CommitRequest> factory =
          () ->
              CommitRequest.newBuilder()
                  .setStartVersion(startTS)
                  .setCommitVersion(commitTS)
                  .addAllKeys(Collections.singletonList(k))
                  .setContext(pair.first.getContext())
                  .build();

      KVErrorHandler<CommitResponse> handler =
          new KVErrorHandler<>(
              session.getRegionManager(),
              client,
              pair.first,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      CommitResponse resp =
          client.callWithRetry(backOffer, TikvGrpc.METHOD_KV_COMMIT, factory, handler);

      if (resp.hasRegionError()) {
        throw new RegionException(resp.getRegionError());
      }

      if (resp.hasError()) {
        throw new KeyException(resp.getError());
      }
    }
    return true;
  }

  private void putAlphabet() {
    for (int i = 0; i < 26; i++) {
      long startTs = pdClient.getTimestamp(backOffer).getVersion();
      long endTs = pdClient.getTimestamp(backOffer).getVersion();
      while (startTs == endTs) {
        endTs = pdClient.getTimestamp(backOffer).getVersion();
      }
      putKV(String.valueOf((char) ('a' + i)), String.valueOf((char) ('a' + i)), startTs, endTs);
    }
    versionTest();
  }

  private void prepareAlphabetLocks() {
    TiTimestamp startTs = pdClient.getTimestamp(backOffer);
    TiTimestamp endTs = pdClient.getTimestamp(backOffer);
    while (startTs == endTs) {
      endTs = pdClient.getTimestamp(backOffer);
    }
    putKV("c", "cc", startTs.getVersion(), endTs.getVersion());
    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);
    while (startTs == endTs) {
      endTs = pdClient.getTimestamp(backOffer);
    }

    assertTrue(lockKey("c", "c", "z1", "z1", true, startTs.getVersion(), endTs.getVersion()));
    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);
    while (startTs == endTs) {
      endTs = pdClient.getTimestamp(backOffer);
    }
    assertTrue(lockKey("d", "dd", "z2", "z2", false, startTs.getVersion(), endTs.getVersion()));
  }

  @Before
  public void setUp() {
    TiConfiguration conf = TiConfiguration.createDefault("127.0.0.1:2379");
    try {
      session = TiSession.create(conf);
      pdClient = PDClient.create(session);
    } catch (Exception e) {
      logger.warn("TiDB cluster may not be present");
      // ignore npe since this test requires tidb cluster being present.
      return;
    }
  }

  @Test
  public void getSITest() {
    session.getConf().setIsolationLevel(IsolationLevel.SI);
    putAlphabet();
    prepareAlphabetLocks();

    versionTest();
  }

  private void versionTest() {
    for (int i = 0; i < 26; i++) {
      Pair<TiRegion, Store> pair =
          session
              .getRegionManager()
              .getRegionStorePairByKey(ByteString.copyFromUtf8(String.valueOf((char) ('a' + i))));
      RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, session);
      ByteString v =
          client.get(
              backOffer,
              ByteString.copyFromUtf8(String.valueOf((char) ('a' + i))),
              pdClient.getTimestamp(backOffer).getVersion());
      assertEquals(v.toStringUtf8(), String.valueOf((char) ('a' + i)));
    }
  }

  @Test
  public void getRCTest() {
    session.getConf().setIsolationLevel(IsolationLevel.RC);
    putAlphabet();
    prepareAlphabetLocks();

    versionTest();
  }

  @Test
  public void cleanLockTest() {
    session.getConf().setIsolationLevel(IsolationLevel.SI);
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
      Pair<TiRegion, Store> pair =
          session
              .getRegionManager()
              .getRegionStorePairByKey(ByteString.copyFromUtf8(String.valueOf((char) ('a' + i))));
      RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, session);
      ByteString v =
          client.get(
              backOffer,
              ByteString.copyFromUtf8(String.valueOf((char) ('a' + i))),
              pdClient.getTimestamp(backOffer).getVersion());
      assertEquals(v.toStringUtf8(), String.valueOf((char) ('a' + i + 1)));
    }

    session.getConf().setIsolationLevel(IsolationLevel.RC);
  }

  @Test
  public void txnStatusTest() {
    session.getConf().setIsolationLevel(IsolationLevel.SI);
    TiTimestamp startTs = pdClient.getTimestamp(backOffer);
    TiTimestamp endTs = pdClient.getTimestamp(backOffer);

    putKV("a", "a", startTs.getVersion(), endTs.getVersion());
    Pair<TiRegion, Store> pair =
        session
            .getRegionManager()
            .getRegionStorePairByKey(ByteString.copyFromUtf8(String.valueOf('a')));
    RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, session);
    long status =
        client.lockResolverClient.getTxnStatus(
            backOffer, startTs.getVersion(), ByteString.copyFromUtf8(String.valueOf('a')));
    assertEquals(status, endTs.getVersion());

    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);

    lockKey("a", "a", "a", "a", true, startTs.getVersion(), endTs.getVersion());
    pair =
        session
            .getRegionManager()
            .getRegionStorePairByKey(ByteString.copyFromUtf8(String.valueOf('a')));
    client = RegionStoreClient.create(pair.first, pair.second, session);
    status =
        client.lockResolverClient.getTxnStatus(
            backOffer, startTs.getVersion(), ByteString.copyFromUtf8(String.valueOf('a')));
    assertEquals(status, endTs.getVersion());

    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);

    lockKey("a", "a", "a", "a", false, startTs.getVersion(), endTs.getVersion());
    pair =
        session
            .getRegionManager()
            .getRegionStorePairByKey(ByteString.copyFromUtf8(String.valueOf('a')));
    client = RegionStoreClient.create(pair.first, pair.second, session);
    status =
        client.lockResolverClient.getTxnStatus(
            backOffer, startTs.getVersion(), ByteString.copyFromUtf8(String.valueOf('a')));
    assertNotSame(status, endTs.getVersion());

    session.getConf().setIsolationLevel(IsolationLevel.RC);
  }

  @Test
  public void SITest() {
    session.getConf().setIsolationLevel(IsolationLevel.SI);
    TiTimestamp startTs = pdClient.getTimestamp(backOffer);
    TiTimestamp endTs = pdClient.getTimestamp(backOffer);

    putKV("a", "a", startTs.getVersion(), endTs.getVersion());

    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);

    lockKey("a", "aa", "a", "aa", false, startTs.getVersion(), endTs.getVersion());

    Pair<TiRegion, Store> pair =
        session
            .getRegionManager()
            .getRegionStorePairByKey(ByteString.copyFromUtf8(String.valueOf('a')));
    RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, session);
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
      assertNotNull(e.getKeyErr().getRetryable());
    }
    session.getConf().setIsolationLevel(IsolationLevel.RC);
  }

  @Test
  public void RCTest() {
    session.getConf().setIsolationLevel(IsolationLevel.RC);
    TiTimestamp startTs = pdClient.getTimestamp(backOffer);
    TiTimestamp endTs = pdClient.getTimestamp(backOffer);

    putKV("a", "a", startTs.getVersion(), endTs.getVersion());

    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);

    lockKey("a", "aa", "a", "aa", false, startTs.getVersion(), endTs.getVersion());

    Pair<TiRegion, Store> pair =
        session
            .getRegionManager()
            .getRegionStorePairByKey(ByteString.copyFromUtf8(String.valueOf('a')));
    RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, session);
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
    } catch (KeyException e) {
      fail();
    }
  }
}
