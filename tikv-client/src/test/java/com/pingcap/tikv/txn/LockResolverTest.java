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
import static junit.framework.TestCase.*;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.ReadOnlyPDClient;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.operation.KVErrorHandler;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.tikv.kvproto.Kvrpcpb.*;
import org.tikv.kvproto.TikvGrpc;

public abstract class LockResolverTest {
  private final Logger logger = Logger.getLogger(this.getClass());
  TiSession session;
  private static final int DefaultTTL = 10;
  BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(1000);
  ReadOnlyPDClient pdClient;
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
    res = commit(startTS, commitTS, Collections.singletonList(ByteString.copyFromUtf8(key)));
    assertTrue(res);
  }

  boolean prewrite(List<Mutation> mutations, long startTS, Mutation primary) {
    if (mutations.size() == 0) return true;

    for (Mutation m : mutations) {
      TiRegion region = session.getRegionManager().getRegionByKey(m.getKey());
      RegionStoreClient client = builder.build(region);

      Supplier<PrewriteRequest> factory =
          () ->
              PrewriteRequest.newBuilder()
                  .addAllMutations(Collections.singletonList(m))
                  .setPrimaryLock(primary.getKey())
                  .setStartVersion(startTS)
                  .setLockTtl(DefaultTTL)
                  .setContext(region.getContext())
                  .build();

      KVErrorHandler<PrewriteResponse> handler =
          new KVErrorHandler<>(
              session.getRegionManager(),
              client,
              client.lockResolverClient,
              region,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null,
              resp -> null);
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

  boolean commit(long startTS, long commitTS, List<ByteString> keys) {
    if (keys.size() == 0) return true;

    for (ByteString k : keys) {
      TiRegion tiRegion = session.getRegionManager().getRegionByKey(k);

      RegionStoreClient client = builder.build(tiRegion);
      Supplier<CommitRequest> factory =
          () ->
              CommitRequest.newBuilder()
                  .setStartVersion(startTS)
                  .setCommitVersion(commitTS)
                  .addAllKeys(Collections.singletonList(k))
                  .setContext(tiRegion.getContext())
                  .build();

      KVErrorHandler<CommitResponse> handler =
          new KVErrorHandler<>(
              session.getRegionManager(),
              client,
              client.lockResolverClient,
              tiRegion,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null,
              resp -> null);
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

  void putAlphabet() {
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

  void prepareAlphabetLocks() {
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
      try {
        ByteString v = client.get(backOffer, key, pdClient.getTimestamp(backOffer).getVersion());
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
