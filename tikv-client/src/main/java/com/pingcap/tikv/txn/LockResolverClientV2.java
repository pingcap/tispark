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
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.operation.KVErrorHandler;
import com.pingcap.tikv.region.AbstractRegionStoreClient;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.region.TiRegion.RegionVerID;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ChannelFactory;
import com.pingcap.tikv.util.TsoUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb.CleanupRequest;
import org.tikv.kvproto.Kvrpcpb.CleanupResponse;
import org.tikv.kvproto.Kvrpcpb.ResolveLockRequest;
import org.tikv.kvproto.Kvrpcpb.ResolveLockResponse;
import org.tikv.kvproto.TikvGrpc;
import org.tikv.kvproto.TikvGrpc.TikvBlockingStub;
import org.tikv.kvproto.TikvGrpc.TikvStub;

/** Before v3.0.5 TiDB uses the ttl on secondary lock. */
public class LockResolverClientV2 extends AbstractRegionStoreClient
    implements AbstractLockResolverClient {
  private static final Logger logger = LoggerFactory.getLogger(LockResolverClientV2.class);

  private final ReadWriteLock readWriteLock;

  /**
   * Note: Because the internal of long is same as unsigned_long and Txn id are never changed. Be
   * careful to compare between two tso the `resolved` mapping is as {@code Map<TxnId, TxnStatus>}
   * TxnStatus represents a txn's final status. It should be Commit or Rollback. if TxnStatus > 0,
   * means the commit ts, otherwise abort
   */
  private final Map<Long, Long> resolved;

  /** the list is chain of txn for O(1) lru cache */
  private final Queue<Long> recentResolved;

  public LockResolverClientV2(
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

  private Long getTxnStatus(BackOffer bo, Long txnID, ByteString primary) {
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
              resp -> resp.hasError() ? resp.getError() : null,
              resolveLockResult -> null,
              0L,
              false);
      CleanupResponse resp = callWithRetry(bo, TikvGrpc.getKvCleanupMethod(), factory, handler);

      status = 0L;

      if (resp == null) {
        logger.error("getKvCleanupMethod failed without a cause");
        regionManager.onRequestFail(region);
        bo.doBackOff(
            BoRegionMiss,
            new TiClientInternalException("getKvCleanupMethod failed without a cause"));
        continue;
      }

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

  @Override
  public String getVersion() {
    return "V2";
  }

  @Override
  public ResolveLockResult resolveLocks(
      BackOffer bo, long callerStartTS, List<Lock> locks, boolean forWrite) {
    if (doResolveLocks(bo, locks)) {
      return new ResolveLockResult(0L);
    } else {
      return new ResolveLockResult(10000L);
    }
  }

  private boolean doResolveLocks(BackOffer bo, List<Lock> locks) {
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
    // TODO: Maybe put it in all LockResolverClientV2 and share by all txns.
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
              resp -> resp.hasError() ? resp.getError() : null,
              resolveLockResult -> null,
              0L,
              false);
      ResolveLockResponse resp =
          callWithRetry(bo, TikvGrpc.getKvResolveLockMethod(), factory, handler);

      if (resp == null) {
        logger.error("getKvResolveLockMethod failed without a cause");
        regionManager.onRequestFail(region);
        bo.doBackOff(
            BoRegionMiss,
            new TiClientInternalException("getKvResolveLockMethod failed without a cause"));
        continue;
      }

      if (resp.hasRegionError()) {
        bo.doBackOff(BoRegionMiss, new RegionException(resp.getRegionError()));
        continue;
      }

      if (resp.hasError()) {
        logger.error(
            String.format("unexpected resolveLock err: %s, lock: %s", resp.getError(), lock));
        throw new KeyException(resp.getError());
      }

      cleanRegion.add(region.getVerID());
      return;
    }
  }
}
