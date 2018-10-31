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

import com.google.protobuf.ByteString;
import com.pingcap.tikv.PDClient;
import com.pingcap.tikv.ReadOnlyPDClient;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.kvproto.Kvrpcpb.IsolationLevel;
import com.pingcap.tikv.kvproto.Kvrpcpb.Op;
import com.pingcap.tikv.kvproto.Kvrpcpb.KeyError;
import com.pingcap.tikv.kvproto.Kvrpcpb.PrewriteResponse;
import com.pingcap.tikv.kvproto.Kvrpcpb.Mutation;
import com.pingcap.tikv.kvproto.Kvrpcpb.PrewriteRequest;
import com.pingcap.tikv.kvproto.Kvrpcpb.CommitResponse;
import com.pingcap.tikv.kvproto.Kvrpcpb.CommitRequest;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.kvproto.TikvGrpc;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.operation.KVErrorHandler;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.function.Supplier;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class LockResolverTest {
  private TiSession session;
  private static final String LOCAL_ADDR = "127.0.0.1";
  private static final String port = "20160";
  private BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(1000);
  private ReadOnlyPDClient pdClient;

  public void putKV(String key, String value, long startTS, long commitTS) {
    Mutation m = Mutation.newBuilder().setKey(ByteString.copyFromUtf8(key))
        .setOp(Op.Put).setValue(ByteString.copyFromUtf8(value)).build();

    boolean res = prewrite(Arrays.asList(m), startTS, m);
    assertTrue(res);
    res = commit(startTS, commitTS, Arrays.asList(ByteString.copyFromUtf8(key)));
    assertTrue(res);
  }

  public boolean prewrite(List<Mutation> mutations, long startTS, Mutation primary) {
    if (mutations.size() == 0)
      return true;

    Map<TiRegion, List<Mutation>> batchs = new HashMap<>();
    Store store = null;

    for (Mutation m: mutations) {
      Pair<TiRegion, Store> pair = session.getRegionManager().
          getRegionStorePairByKey(m.getKey());
      store = pair.second;
      batchs.putIfAbsent(pair.first, new ArrayList<>());
      batchs.get(pair.first).add(m);
    }

    for (TiRegion r: batchs.keySet()) {
      if (!prewriteBatch(batchs.get(r), startTS, primary, r, store)) {
        return false;
      }
    }
    return true;
  }

  public boolean prewriteBatch(List<Mutation> mutations, long startTS, Mutation primary, TiRegion region, Store store) {
    RegionStoreClient client = RegionStoreClient.create(region, store, session);

    Supplier<PrewriteRequest> factory = () ->
        PrewriteRequest.newBuilder()
            .addAllMutations(mutations)
            .setPrimaryLock(primary.getKey())
            .setStartVersion(startTS)
            .setLockTtl(3000)
            .setContext(region.getContext()).build();

    KVErrorHandler<PrewriteResponse> handler =
        new KVErrorHandler<>(
            session.getRegionManager(),
            client.getSender(),
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null
        );

    PrewriteResponse resp = client.getSender().callWithRetry(backOffer, TikvGrpc.METHOD_KV_PREWRITE, factory, handler);

    if (resp.hasRegionError()) {
      return prewrite(mutations, startTS, primary);
    }

    if (resp.getErrorsCount() == 0) {
      return true;
    }

    List<Lock> locks = new ArrayList<>();
    for (KeyError err : resp.getErrorsList()) {
      if (err.hasLocked()) {
        Lock lock = new Lock(err.getLocked());
        locks.add(lock);
      }
    }

    return client.lockResolver.ResolveLocks(backOffer, locks);
  }

  public boolean lockKey(String key, String value, String primaryKey,
                      String primaryValue, boolean commitPrimary, long startTs, long commitTS) {
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(Mutation.newBuilder().setKey(ByteString.copyFromUtf8(primaryKey))
        .setValue(ByteString.copyFromUtf8(primaryValue)).setOp(Op.Put).build());
    mutations.add(Mutation.newBuilder().setKey(ByteString.copyFromUtf8(key))
        .setValue(ByteString.copyFromUtf8(value)).setOp(Op.Put).build());
    if (!prewrite(mutations, startTs, mutations.get(0)))
      return false;

    if (commitPrimary) {
      if (!commit(startTs, commitTS, Arrays.asList(ByteString.copyFromUtf8(primaryKey), ByteString.copyFromUtf8(key)))) {
        return false;
      }
    }

    return true;
  }

  public boolean commit(long startTS, long commitTS, List<ByteString> keys) {
    if (keys.size() == 0)
      return true;

    Map<TiRegion, List<ByteString>> batchs = new HashMap<>();
    Store store = null;

    for (ByteString k: keys) {
      Pair<TiRegion, Store> pair = session.getRegionManager().
          getRegionStorePairByKey(k);
      store = pair.second;
      List<ByteString> batch = batchs.get(pair.first);
      if (batch == null) {
        batch = new ArrayList<>();
        batchs.put(pair.first, batch);
      }

      batch.add(k);
    }

    for (TiRegion r: batchs.keySet()) {
      if (!commitBatch(startTS, commitTS, batchs.get(r), r, store)) {
        return false;
      }
    }
    return true;
  }

  public boolean commitBatch(long startTS, long commitTS, List<ByteString> keys, TiRegion region, Store store) {
    RegionStoreClient client = RegionStoreClient.create(region, store, session);
    Supplier<CommitRequest> factory = () ->
        CommitRequest.newBuilder()
            .setStartVersion(startTS)
            .setCommitVersion(commitTS)
            .addAllKeys(keys)
            .setContext(region.getContext())
            .build();

    KVErrorHandler<CommitResponse> handler =
        new KVErrorHandler<>(
            session.getRegionManager(),
            client.getSender(),
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null
        );

    CommitResponse resp = client.getSender().callWithRetry(backOffer, TikvGrpc.METHOD_KV_COMMIT, factory, handler);

    if (resp.hasRegionError()) {
      return commit(startTS, commitTS, keys);
    }

    if (resp.hasError()) {
      return false;
    }

    return true;
  }

  public void putAlphabet() {
    for (int i = 0; i < 26; i++) {
      long startTs = pdClient.getTimestamp(backOffer).getVersion();
      long endTs = pdClient.getTimestamp(backOffer).getVersion();
      while (startTs == endTs) {
        endTs = pdClient.getTimestamp(backOffer).getVersion();
      }
      putKV(String.valueOf((char)('a' + i)), String.valueOf((char)('a' + i)), startTs, endTs);
    }
    for (int i = 0; i < 26; i++) {
      Pair<TiRegion, Store> pair = session.getRegionManager().
          getRegionStorePairByKey(ByteString.copyFromUtf8(String.valueOf((char)('a' + i))));
      RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, session);
      ByteString v = client.get(backOffer, ByteString.copyFromUtf8(String.valueOf((char)('a' + i))), pdClient.getTimestamp(backOffer).getVersion());
      assertEquals(v.toStringUtf8(), String.valueOf((char)('a' + i)));
    }
  }

  public void prepareAlphabetLocks() {
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

    assertTrue(lockKey("c", "c", "z1", "z1", true,
        startTs.getVersion(), endTs.getVersion()));
    startTs = pdClient.getTimestamp(backOffer);
    endTs = pdClient.getTimestamp(backOffer);
    while (startTs == endTs) {
      endTs = pdClient.getTimestamp(backOffer);
    }
    assertTrue(lockKey("d", "dd", "z2", "z2", false,
        startTs.getVersion(), endTs.getVersion()));
  }

  @Before
  public void setUp() throws Exception {
    TiConfiguration conf = TiConfiguration.createDefault("127.0.0.1:2379");
    session = TiSession.create(conf);
    pdClient = PDClient.create(session);
  }

  @Test
  public void getSITest() throws Exception {
    session.getConf().setIsolationLevel(IsolationLevel.SI);
    putAlphabet();
    prepareAlphabetLocks();

    for (int i = 0; i < 26; i++) {
      Pair<TiRegion, Store> pair = session.getRegionManager().
          getRegionStorePairByKey(ByteString.copyFromUtf8(String.valueOf((char)('a' + i))));
      RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, session);
      ByteString v = client.get(backOffer,
          ByteString.copyFromUtf8(String.valueOf((char)('a' + i))), pdClient.getTimestamp(backOffer).getVersion());
      assertEquals(v.toStringUtf8(), String.valueOf((char)('a' + i)));
    }
  }

  @Test
  public void getRCTest() {
    session.getConf().setIsolationLevel(IsolationLevel.RC);
    putAlphabet();
    prepareAlphabetLocks();

    for (int i = 0; i < 26; i++) {
      Pair<TiRegion, Store> pair = session.getRegionManager().
          getRegionStorePairByKey(ByteString.copyFromUtf8(String.valueOf((char)('a' + i))));
      RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, session);
      ByteString v = client.get(backOffer,
          ByteString.copyFromUtf8(String.valueOf((char)('a' + i))), pdClient.getTimestamp(backOffer).getVersion());
      assertEquals(v.toStringUtf8(), String.valueOf((char)('a' + i)));
    }
  }
}
