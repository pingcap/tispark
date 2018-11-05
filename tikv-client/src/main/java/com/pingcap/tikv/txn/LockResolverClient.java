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
import com.pingcap.tikv.AbstractGRPCClient;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.kvproto.Kvrpcpb.CleanupRequest;
import com.pingcap.tikv.kvproto.Kvrpcpb.CleanupResponse;
import com.pingcap.tikv.kvproto.Kvrpcpb.ResolveLockRequest;
import com.pingcap.tikv.kvproto.Kvrpcpb.ResolveLockResponse;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.kvproto.TikvGrpc;
import com.pingcap.tikv.kvproto.TikvGrpc.TikvStub;
import com.pingcap.tikv.kvproto.TikvGrpc.TikvBlockingStub;
import com.pingcap.tikv.operation.KVErrorHandler;
import com.pingcap.tikv.region.RegionErrorReceiver;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.region.TiRegion.RegionVerID;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.TsoUtils;
import io.grpc.ManagedChannel;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoRegionMiss;

// LockResolver resolves locks and also caches resolved txn status.
public class LockResolverClient extends AbstractGRPCClient<TikvBlockingStub, TikvStub> implements RegionErrorReceiver {
  // ResolvedCacheSize is max number of cached txn status.
  private static final long resolvedTxnCacheSize = 2048;
  // By default, locks after 3000ms is considered unusual (the client created the
  // lock might be dead). Other client may cleanup this kind of lock.
  // For locks created recently, we will do backoff and retry.
  private static final long defaultLockTTL = 3000;
  private static final long maxLockTTL = 120000;
  // ttl = ttlFactor * sqrt(writeSizeInMiB)
  private static final long ttlFactor = 6000;
  private static final Logger logger = Logger.getLogger(LockResolverClient.class);

  private final ReadWriteLock readWriteLock;
  // Note: Because the internal of long is same as unsigned_long
  // and Txn id are never changed. Be careful to compare between two tso
  // the `resolved` mapping is as {@code Map<TxnId, TxnStatus>}
  // TxnStatus represents a txn's final status. It should be Commit or Rollback.
  // if TxnStatus > 0, means the commit ts, otherwise abort
  private final Map<Long, Long> resolved;
  // the list is chain of txn for O(1) lru cache
  private final LinkedList<Long> recentResolved;
  private TikvBlockingStub blockingStub;
  private TikvStub asyncStub;
  private TiRegion region;

  public LockResolverClient(TiSession session,
                            TikvBlockingStub blockingStub,
                            TikvStub asyncStub) {
    super(session);
    resolved = new HashMap<>();
    recentResolved = new LinkedList<>();
    readWriteLock = new ReentrantReadWriteLock();
    this.blockingStub = blockingStub;
    this.asyncStub = asyncStub;
  }

  private void saveResolved(long txnID, long status) {
    try {
      readWriteLock.writeLock().lock();
      if (resolved.containsKey(txnID)) {
        return ;
      }

      resolved.put(txnID, status);
      recentResolved.addLast(txnID);
      if (recentResolved.size() > resolvedTxnCacheSize) {
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

  public Long getTxnStatus(BackOffer bo, Long txnID, ByteString primary) {
    Long status = getResolved(txnID);

    if (status != null) {
      return status;
    }

    while (true) {
      // refresh region
      region = session.getRegionManager().getRegionByKey(primary);

      Supplier<CleanupRequest> factory = () ->
          CleanupRequest.newBuilder().setContext(region.getContext()).setKey(primary).setStartVersion(txnID).build();
      KVErrorHandler<CleanupResponse> handler =
          new KVErrorHandler<>(
              session.getRegionManager(), this, region, resp -> resp.hasRegionError() ? resp.getRegionError() : null);

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
  //    are expired then all locks will be resolved so tru will be returned, otherwise
  //    caller should sleep a while before retry.
  // 2) For each lock, query the primary key to get txn(which left the lock)'s
  //    commit status.
  // 3) Send `ResolveLock` cmd to the lock's region to resolve all locks belong to
  //    the same transaction.
  public boolean resolveLocks(BackOffer bo, List<Lock> locks) {
    if (locks.size() == 0) {
      return true;
    }

    List<Lock> expiredLocks = new ArrayList<>();
    for (Lock lock: locks) {
      if (TsoUtils.isExpired(lock.getTxnID(), lock.getTtl())) {
        expiredLocks.add(lock);
      }
    }

    if (expiredLocks.size() == 0) {
      return false;
    }

    // TxnID -> []Region, record resolved Regions.
    // TODO: Maybe put it in LockResolver and share by all txns.
    Map<Long, Set<RegionVerID>> cleanTxns = new HashMap<>();
    for (Lock l: expiredLocks) {
      Long status = getTxnStatus(bo, l.getTxnID(), l.getPrimary());

      cleanTxns.putIfAbsent(l.getTxnID(), new HashSet<>());
      Set<RegionVerID> cleanRegion = cleanTxns.get(l.getTxnID());

      resolveLock(bo, l, status, cleanRegion);
    }

    return expiredLocks.size() == locks.size();
  }

  private void resolveLock(BackOffer bo, Lock lock, long txnStatus, Set<RegionVerID> cleanRegion) {

    while (true) {
      region = session.getRegionManager().getRegionByKey(lock.getKey());

      if (cleanRegion.contains(region.getVerID())) {
        return ;
      }

      Supplier<ResolveLockRequest> factory;

      if (txnStatus > 0) {
        // txn is commited with commitTS txnStatus
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
              session.getRegionManager(), this, region, resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      ResolveLockResponse resp = callWithRetry(bo, TikvGrpc.METHOD_KV_RESOLVE_LOCK, factory, handler);

      if (resp.hasError()) {
        logger.error(String.format("unexpected resolveLock err: %s, lock: %s", resp.getError(), lock));
        throw new KeyException(resp.getError());
      }

      if (resp.hasRegionError()) {
        bo.doBackOff(BoRegionMiss, new RegionException(resp.getRegionError()));
        continue;
      }

      cleanRegion.add(region.getVerID());
      return ;
    }
  }

  @Override
  protected TikvBlockingStub getBlockingStub() {
    return blockingStub.withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
  }

  @Override
  protected TikvStub getAsyncStub() {
    return asyncStub.withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
  }

  @Override
  public void close() throws Exception {
  }

  /**
   * onNotLeader deals with NotLeaderError and returns whether re-splitting key range is needed
   *
   * @param newStore the new store presented by NotLeader Error
   * @return false when re-split is needed.
   */
  @Override
  public boolean onNotLeader(Store newStore) {
    if (logger.isDebugEnabled()) {
      logger.debug(region + ", new leader = " + newStore.getId());
    }
    TiRegion cachedRegion = session.getRegionManager().getRegionById(region.getId());
    // When switch leader fails or the region changed its key range,
    // it would be necessary to re-split task's key range for new region.
    if (!region.switchPeer(newStore.getId()) ||
        !region.getStartKey().equals(cachedRegion.getStartKey()) ||
        !region.getEndKey().equals(cachedRegion.getEndKey())) {
      return false;
    }
    String addressStr = newStore.getAddress();
    ManagedChannel channel = session.getChannel(addressStr);
    blockingStub = TikvGrpc.newBlockingStub(channel);
    asyncStub = TikvGrpc.newStub(channel);
    return true;
  }

  @Override
  public void onStoreNotMatch(Store store) {
    String addressStr = store.getAddress();
    ManagedChannel channel = session.getChannel(addressStr);
    blockingStub = TikvGrpc.newBlockingStub(channel);
    asyncStub = TikvGrpc.newStub(channel);
    if (logger.isDebugEnabled() && region.getLeader().getStoreId() != store.getId()) {
      logger.debug("store_not_match may occur? " + region + ", original store = " + store.getId() + " address = " + addressStr);
    }
  }
}



