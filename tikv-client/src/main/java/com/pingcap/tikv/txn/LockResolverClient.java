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

import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoRegionMiss;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.operation.KVErrorHandler;
import com.pingcap.tikv.region.AbstractRegionStoreClient;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.region.TiRegion.RegionVerID;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ChannelFactory;
import com.pingcap.tikv.util.TsoUtils;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.apache.log4j.Logger;
import org.tikv.kvproto.Kvrpcpb.CleanupRequest;
import org.tikv.kvproto.Kvrpcpb.CleanupResponse;
import org.tikv.kvproto.Kvrpcpb.ResolveLockRequest;
import org.tikv.kvproto.Kvrpcpb.ResolveLockResponse;
import org.tikv.kvproto.TikvGrpc;
import org.tikv.kvproto.TikvGrpc.TikvBlockingStub;
import org.tikv.kvproto.TikvGrpc.TikvStub;

// LockResolver resolves locks and also caches resolved txn status.
public class LockResolverClient extends AbstractRegionStoreClient {
  // ResolvedCacheSize is max number of cached txn status.
  private static final long RESOLVED_TXN_CACHE_SIZE = 2048;
  // By default, locks after 3000ms is considered unusual (the client created the
  // lock might be dead). Other client may cleanup this kind of lock.
  // For locks created recently, we will do backoff and retry.
  private static final long DEFAULT_LOCK_TTL = 3000;
  private static final long MAX_LOCK_TTL = 120000;
  // ttl = ttlFactor * sqrt(writeSizeInMiB)
  private static final long TTL_FACTOR = 6000;
  private static final Logger logger = Logger.getLogger(LockResolverClient.class);

  private final ReadWriteLock readWriteLock;
  // Note: Because the internal of long is same as unsigned_long
  // and Txn id are never changed. Be careful to compare between two tso
  // the `resolved` mapping is as {@code Map<TxnId, TxnStatus>}
  // TxnStatus represents a txn's final status. It should be Commit or Rollback.
  // if TxnStatus > 0, means the commit ts, otherwise abort
  private final Map<Long, Long> resolved;
  // the list is chain of txn for O(1) lru cache
  private final Queue<Long> recentResolved;

  public LockResolverClient(
      TiConfiguration conf,
      TiRegion region,
      TikvBlockingStub blockingStub,
      TikvStub asyncStub,
      ChannelFactory channelFactory,
      RegionManager regionManager) {
    super(conf, region, channelFactory, blockingStub, asyncStub, regionManager);
    resolved = new HashMap<>();
    recentResolved = new LinkedList<>();
    readWriteLock = new ReentrantReadWriteLock();
  }

  private void saveResolved(long txnID, long status) {
    try {
      readWriteLock.writeLock().lock();
      if (resolved.containsKey(txnID)) {
        return;
      }

      resolved.put(txnID, status);
      recentResolved.add(txnID);
      if (recentResolved.size() > RESOLVED_TXN_CACHE_SIZE) {
        Long front = recentResolved.remove();
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

  public Long getTxnStatus(BackOffer bo, Long txnID, ByteString primary) {
    Long status = getResolved(txnID);

    if (status != null) {
      return status;
    }

    while (true) {
      // refresh region
      region = regionManager.getRegionByKey(primary);

      Supplier<CleanupRequest> factory =
          () ->
              CleanupRequest.newBuilder()
                  .setContext(region.getContext())
                  .setKey(primary)
                  .setStartVersion(txnID)
                  .build();
      KVErrorHandler<CleanupResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              this,
              region,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null,
              resp -> null);
      CleanupResponse resp = callWithRetry(bo, TikvGrpc.METHOD_KV_CLEANUP, factory, handler);

      status = 0L;
      if (resp.hasRegionError()) {
        bo.doBackOff(BoRegionMiss, new RegionException(resp.getRegionError()));
        continue;
      }

      if (resp.hasError()) {
        logger.error(String.format("unexpected cleanup err: %s, tid: %d", resp.getError(), txnID));
        throw new KeyException(resp.getError());
      }

      if (resp.getCommitVersion() != 0) {
        status = resp.getCommitVersion();
      }

      saveResolved(txnID, status);
      return status;
    }
  }

  // ResolveLocks tries to resolve Locks. The resolving process is in 3 steps:
  // 1) Use the `lockTTL` to pick up all expired locks. Only locks that are old
  //    enough are considered orphan locks and will be handled later. If all locks
  //    are expired then all locks will be resolved so true will be returned, otherwise
  //    caller should sleep a while before retry.
  // 2) For each lock, query the primary key to get txn(which left the lock)'s
  //    commit status.
  // 3) Send `ResolveLock` cmd to the lock's region to resolve all locks belong to
  //    the same transaction.
  public boolean resolveLocks(BackOffer bo, List<Lock> locks) {
    if (locks.isEmpty()) {
      return true;
    }

    List<Lock> expiredLocks = new ArrayList<>();
    for (Lock lock : locks) {
      if (TsoUtils.isExpired(lock.getTxnID(), lock.getTtl())) {
        expiredLocks.add(lock);
      }
    }

    if (expiredLocks.isEmpty()) {
      return false;
    }

    // TxnID -> []Region, record resolved Regions.
    // TODO: Maybe put it in all LockResolverClient and share by all txns.
    Map<Long, Set<RegionVerID>> cleanTxns = new HashMap<>();
    for (Lock l : expiredLocks) {
      Long status = getTxnStatus(bo, l.getTxnID(), l.getPrimary());

      Set<RegionVerID> cleanRegion = cleanTxns.computeIfAbsent(l.getTxnID(), k -> new HashSet<>());

      resolveLock(bo, l, status, cleanRegion);
    }

    return expiredLocks.size() == locks.size();
  }

  private void resolveLock(BackOffer bo, Lock lock, long txnStatus, Set<RegionVerID> cleanRegion) {

    while (true) {
      region = regionManager.getRegionByKey(lock.getKey());

      if (cleanRegion.contains(region.getVerID())) {
        return;
      }

      Supplier<ResolveLockRequest> factory;

      if (txnStatus > 0) {
        // txn is committed with commitTS txnStatus
        factory =
            () ->
                ResolveLockRequest.newBuilder()
                    .setContext(region.getContext())
                    .setStartVersion(lock.getTxnID())
                    .setCommitVersion(txnStatus)
                    .build();
      } else {
        factory =
            () ->
                ResolveLockRequest.newBuilder()
                    .setContext(region.getContext())
                    .setStartVersion(lock.getTxnID())
                    .build();
      }

      KVErrorHandler<ResolveLockResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              this,
              region,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null,
              resp -> null);
      ResolveLockResponse resp =
          callWithRetry(bo, TikvGrpc.METHOD_KV_RESOLVE_LOCK, factory, handler);

      if (resp.hasError()) {
        logger.error(
            String.format("unexpected resolveLock err: %s, lock: %s", resp.getError(), lock));
        throw new KeyException(resp.getError());
      }

      if (resp.hasRegionError()) {
        bo.doBackOff(BoRegionMiss, new RegionException(resp.getRegionError()));
        continue;
      }

      cleanRegion.add(region.getVerID());
      return;
    }
  }
}
