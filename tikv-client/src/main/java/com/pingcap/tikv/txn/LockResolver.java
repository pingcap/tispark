/*
 *
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
 *
 */

package com.pingcap.tikv.txn;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.kvproto.Kvrpcpb.CleanupRequest;
import com.pingcap.tikv.kvproto.Kvrpcpb.CleanupResponse;
import com.pingcap.tikv.kvproto.Kvrpcpb.ResolveLockRequest;
import com.pingcap.tikv.kvproto.Kvrpcpb.ResolveLockResponse;
import com.pingcap.tikv.kvproto.TikvGrpc;
import com.pingcap.tikv.operation.KVErrorHandler;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.region.TiRegion.RegionVerID;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.Oracle;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

// TODO: metrics for lock resolver
// LockResolver resolves locks and also caches resolved txn status.
public class LockResolver {
  // ResolvedCacheSize is max number of cached txn status.
  private static final long ResolvedCacheSize = 2048;
  // By default, locks after 3000ms is considered unusual (the client created the
  // lock might be dead). Other client may cleanup this kind of lock.
  // For locks created recently, we will do backoff and retry.
  private static long defaultLockTTL = 3000;
  private static long maxLockTTL = 120000;
  // ttl = ttlFactor * sqrt(writeSizeInMiB)
  private static long ttlFactor = 6000;
  private static final Logger logger = Logger.getLogger(LockResolver.class);

  private final ReadWriteLock readWriteLock;
  // Note: Because the internal of long is same as unsigned_long
  // and Txn id are never changed. Be careful to compare between ts

  // the mapping is from TxnId -> TxnStatus
  // TxnStatus represents a txn's final status. It should be Commit or Rollback.
  // if TxnStatus > 0, means the commit ts, otherwise abort
  private final Map<Long, Long> resolved;
  // the list is chain of txn for O(1) lru cache
  private final LinkedList<Long> recentResolved;
  private final RegionStoreClient store;

  public LockResolver(RegionStoreClient regionClient) {
    resolved = new HashMap<>();
    recentResolved = new LinkedList<>();
    readWriteLock = new ReentrantReadWriteLock();
    store = regionClient;
  }

  private void SaveResolved(long txnID, long status) {
    try {
      readWriteLock.writeLock().lock();
      if (resolved.containsKey(txnID)) {
        return ;
      }

      resolved.put(txnID, status);
      recentResolved.addLast(txnID);
      if (recentResolved.size() > ResolvedCacheSize) {
        Long front = recentResolved.removeLast();
        resolved.remove(front);
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private Long getResolved(Long txnID) {
    try {
      readWriteLock.readLock().lock();
      return resolved.get(txnID);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  private Long getTxnStatus(BackOffer bo, Long txnID, ByteString primary) {
    Long status = getResolved(txnID);

    if (status != null) {
      return status;
    }

    // do we need add the BackOffer
    TiRegion region = store.getSession().getRegionManager().getRegionByKey(primary);

    Supplier<CleanupRequest> factory = () ->
        CleanupRequest.newBuilder().setContext(region.getContext()).setKey(primary).setStartVersion(txnID).build();
    KVErrorHandler<CleanupResponse> handler =
        new KVErrorHandler<>(
            store.getSession().getRegionManager(), store, region, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    CleanupResponse resp = store.callWithRetry(bo, TikvGrpc.METHOD_KV_CLEANUP, factory, handler);

    return getTxnStatusHelper(resp, txnID);
  }

  // ResolveLocks tries to resolve Locks. The resolving process is in 3 steps:
  // 1) Use the `lockTTL` to pick up all expired locks. Only locks that are too
  //    old are considered orphan locks and will be handled later. If all locks
  //    are expired then all locks will be resolved so the returned `ok` will be
  //    true, otherwise caller should sleep a while before retry.
  // 2) For each lock, query the primary key to get txn(which left the lock)'s
  //    commit status.
  // 3) Send `ResolveLock` cmd to the lock's region to resolve all locks belong to
  //    the same transaction.
  public boolean ResolveLocks(BackOffer bo, List<Lock> locks) {
    if (locks.size() == 0) {
      return true;
    }

    List<Lock> expiredLocks = new ArrayList<>();
    for (Lock lock: locks) {
      if (Oracle.IsExpired(lock.getTxnID(), lock.getTtl())) {
        expiredLocks.add(lock);
      }
    }

    if (expiredLocks.size() == 0) {
      return false;
    }

    // TxnID -> []Region, record resolved Regions.
    // TODO: Maybe put it in LockResolver and share by all txns.
    Map<Long, Map<RegionVerID, Object>> cleanTxns = new HashMap<>();
    for (Lock l: expiredLocks) {
      Long status = getTxnStatus(bo, l.getTxnID(), l.getPrimary());

      Map<RegionVerID, Object> cleanRegion = cleanTxns.get(l.getTxnID());
      if (cleanRegion == null) {
        cleanRegion = new HashMap<>();
        cleanTxns.put(l.getTxnID(), cleanRegion);
      }

      resolveLock(bo, l, status, cleanRegion);
    }

    return expiredLocks.size() == locks.size();
  }

  private Long getTxnStatusHelper(CleanupResponse resp, Long txnID) {
    long status = 0L;
    if (resp.hasError()) {
      logger.error(String.format("unexpected cleanup err: %s, tid: %d", resp.getError(), txnID));
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }

    if (resp.getCommitVersion() != 0) {
      status = resp.getCommitVersion();
    }

    SaveResolved(txnID, status);
    return status;
  }

  private void resolveLock(BackOffer bo, Lock lock, long txnStatus, Map<RegionVerID, Object> cleanRegion) {
    // TODO: how to deal with while
    // do we need add the BackOffer
    TiRegion region = store.getSession().getRegionManager().getRegionByKey(lock.getKey());

    Supplier<ResolveLockRequest> factory;

    // txn is commited with commitTS txnStatus
    if (txnStatus > 0) {
      factory = () ->
          ResolveLockRequest.newBuilder().setContext(region.getContext()).
              setStartVersion(lock.getTxnID()).setCommitVersion(txnStatus).build();
    } else {
      factory = () ->
          ResolveLockRequest.newBuilder().setContext(region.getContext()).
              setStartVersion(lock.getTxnID()).build();
    }

    KVErrorHandler<ResolveLockResponse> handler =
        new KVErrorHandler<>(
            store.getSession().getRegionManager(), store, region, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    ResolveLockResponse resp = store.callWithRetry(bo, TikvGrpc.METHOD_KV_RESOLVE_LOCK, factory, handler);

    resolveLockHelper(resp, cleanRegion, region, lock);
  }

  private void resolveLockHelper(ResolveLockResponse resp, Map<RegionVerID, Object> cleanRegion, TiRegion region, Lock lock) {
    if (resp.hasError()) {
      logger.error(String.format("unexpected resolveLock err: %s, lock: %s", resp.getError(), lock));
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      // TODO: how to do with region exception
      throw new RegionException(resp.getRegionError());
    }

    // TODO: should we get rid of Object?
    cleanRegion.put(region.getVerID(), new Object());
  }
}



