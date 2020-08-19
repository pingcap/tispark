/*
 *
 * Copyright 2020 PingCAP, Inc.
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
import com.pingcap.tikv.PDClient;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.operation.KVErrorHandler;
import com.pingcap.tikv.region.AbstractRegionStoreClient;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.RegionStoreClient;
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
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.CleanupRequest;
import org.tikv.kvproto.Kvrpcpb.CleanupResponse;
import org.tikv.kvproto.TikvGrpc;
import org.tikv.kvproto.TikvGrpc.TikvBlockingStub;
import org.tikv.kvproto.TikvGrpc.TikvStub;

/** Since v3.0.5 TiDB ignores the ttl on secondary lock and will use the ttl on primary key. */
public class LockResolverClientV3 extends AbstractRegionStoreClient
    implements AbstractLockResolverClient {
  private static final Logger logger = LoggerFactory.getLogger(LockResolverClientV3.class);

  private final ReadWriteLock readWriteLock;

  /**
   * Note: Because the internal of long is same as unsigned_long and Txn id are never changed. Be
   * careful to compare between two tso the `resolved` mapping is as {@code Map<TxnId, TxnStatus>}
   * TxnStatus represents a txn's final status. It should be Commit or Rollback. if TxnStatus > 0,
   * means the commit ts, otherwise abort
   */
  private final Map<Long, TxnStatus> resolved;

  /** the list is chain of txn for O(1) lru cache */
  private final Queue<Long> recentResolved;

  private final PDClient pdClient;

  private final RegionStoreClient.RegionStoreClientBuilder clientBuilder;

  public LockResolverClientV3(
      TiConfiguration conf,
      TiRegion region,
      TikvBlockingStub blockingStub,
      TikvStub asyncStub,
      ChannelFactory channelFactory,
      RegionManager regionManager,
      PDClient pdClient,
      RegionStoreClient.RegionStoreClientBuilder clientBuilder) {
    super(conf, region, channelFactory, blockingStub, asyncStub, regionManager);
    resolved = new HashMap<>();
    recentResolved = new LinkedList<>();
    readWriteLock = new ReentrantReadWriteLock();
    this.pdClient = pdClient;
    this.clientBuilder = clientBuilder;
  }

  @Override
  public String getVersion() {
    return "V3";
  }

  @Override
  public ResolveLockResult resolveLocks(
      BackOffer bo, long callerStartTS, List<Lock> locks, boolean forWrite) {
    TxnExpireTime msBeforeTxnExpired = new TxnExpireTime();

    if (locks.isEmpty()) {
      return new ResolveLockResult(msBeforeTxnExpired.value());
    }

    List<Lock> expiredLocks = new ArrayList<>();
    for (Lock lock : locks) {
      if (TsoUtils.isExpired(lock.getTxnID(), lock.getTtl())) {
        expiredLocks.add(lock);
      } else {
        msBeforeTxnExpired.update(lock.getTtl());
      }
    }

    if (expiredLocks.isEmpty()) {
      return new ResolveLockResult(msBeforeTxnExpired.value());
    }

    Map<Long, Set<RegionVerID>> cleanTxns = new HashMap<>();
    for (Lock l : expiredLocks) {
      TxnStatus status = getTxnStatusFromLock(bo, l);

      if (status.getTtl() == 0) {
        Set<RegionVerID> cleanRegion =
            cleanTxns.computeIfAbsent(l.getTxnID(), k -> new HashSet<>());

        resolveLock(bo, l, status, cleanRegion);
      } else {
        long msBeforeLockExpired = TsoUtils.untilExpired(l.getTxnID(), status.getTtl());
        msBeforeTxnExpired.update(msBeforeLockExpired);
      }
    }

    return new ResolveLockResult(msBeforeTxnExpired.value());
  }

  private void resolveLock(
      BackOffer bo, Lock lock, TxnStatus txnStatus, Set<RegionVerID> cleanRegion) {
    boolean cleanWholeRegion = lock.getTxnSize() >= BIG_TXN_THRESHOLD;

    while (true) {
      region = regionManager.getRegionByKey(lock.getKey());

      if (cleanRegion.contains(region.getVerID())) {
        return;
      }

      Kvrpcpb.ResolveLockRequest.Builder builder =
          Kvrpcpb.ResolveLockRequest.newBuilder()
              .setContext(region.getContext())
              .setStartVersion(lock.getTxnID());

      if (txnStatus.isCommitted()) {
        // txn is committed with commitTS txnStatus
        builder.setCommitVersion(txnStatus.getCommitTS());
      }

      if (lock.getTxnSize() < BIG_TXN_THRESHOLD) {
        // Only resolve specified keys when it is a small transaction,
        // prevent from scanning the whole region in this case.
        builder.addKeys(lock.getKey());
      }

      Supplier<Kvrpcpb.ResolveLockRequest> factory = builder::build;
      KVErrorHandler<Kvrpcpb.ResolveLockResponse> handler =
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
      Kvrpcpb.ResolveLockResponse resp =
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

      if (cleanWholeRegion) {
        cleanRegion.add(region.getVerID());
      }
      return;
    }
  }

  private TxnStatus getTxnStatusFromLock(BackOffer bo, Lock lock) {
    // NOTE: l.TTL = 0 is a special protocol!!!
    // When the pessimistic txn prewrite meets locks of a txn, it should rollback that txn
    // **unconditionally**.
    // In this case, TiKV set the lock TTL = 0, and TiDB use currentTS = 0 to call
    // getTxnStatus, and getTxnStatus with currentTS = 0 would rollback the transaction.
    if (lock.getTtl() == 0) {
      return getTxnStatus(bo, lock.getTxnID(), lock.getPrimary(), 0L);
    }

    long currentTS = pdClient.getTimestamp(bo).getVersion();
    return getTxnStatus(bo, lock.getTxnID(), lock.getPrimary(), currentTS);
  }

  private TxnStatus getTxnStatus(BackOffer bo, Long txnID, ByteString primary, Long currentTS) {
    TxnStatus status = getResolved(txnID);
    if (status != null) {
      return status;
    }

    Supplier<CleanupRequest> factory =
        () -> {
          TiRegion primaryKeyRegion = regionManager.getRegionByKey(primary);
          return CleanupRequest.newBuilder()
              .setContext(primaryKeyRegion.getContext())
              .setKey(primary)
              .setStartVersion(txnID)
              .setCurrentTs(currentTS)
              .build();
        };

    status = new TxnStatus();
    while (true) {
      TiRegion primaryKeyRegion = regionManager.getRegionByKey(primary);
      // new RegionStoreClient for PrimaryKey
      RegionStoreClient primaryKeyRegionStoreClient = clientBuilder.build(primary);
      KVErrorHandler<CleanupResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              primaryKeyRegionStoreClient,
              primaryKeyRegionStoreClient.lockResolverClient,
              primaryKeyRegion,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null,
              resp -> resp.hasError() ? resp.getError() : null,
              resolveLockResult -> null,
              0L,
              false);

      CleanupResponse resp =
          primaryKeyRegionStoreClient.callWithRetry(
              bo, TikvGrpc.getKvCleanupMethod(), factory, handler);

      if (resp == null) {
        logger.error("getKvCleanupMethod failed without a cause");
        regionManager.onRequestFail(primaryKeyRegion);
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
        Kvrpcpb.KeyError keyError = resp.getError();

        // If the TTL of the primary lock is not outdated, the proto returns a ErrLocked contains
        // the TTL.
        if (keyError.hasLocked()) {
          Kvrpcpb.LockInfo lockInfo = keyError.getLocked();
          return new TxnStatus(lockInfo.getLockTtl(), 0L);
        }

        logger.error(String.format("unexpected cleanup err: %s, tid: %d", keyError, txnID));
        throw new KeyException(keyError);
      }

      if (resp.getCommitVersion() != 0) {
        status = new TxnStatus(0L, resp.getCommitVersion());
      }

      saveResolved(txnID, status);
      return status;
    }
  }

  private void saveResolved(long txnID, TxnStatus status) {
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

  private TxnStatus getResolved(Long txnID) {
    try {
      readWriteLock.readLock().lock();
      return resolved.get(txnID);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }
}
