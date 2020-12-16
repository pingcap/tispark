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
import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoTxnNotFound;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.PDClient;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.Utils;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.exception.TiKVException;
import com.pingcap.tikv.exception.TxnNotFoundException;
import com.pingcap.tikv.exception.WriteConflictException;
import com.pingcap.tikv.operation.KVErrorHandler;
import com.pingcap.tikv.region.AbstractRegionStoreClient;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.region.TiRegion.RegionVerID;
import com.pingcap.tikv.txn.type.GroupKeyResult;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ChannelFactory;
import com.pingcap.tikv.util.Pair;
import com.pingcap.tikv.util.TsoUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.TikvGrpc;
import org.tikv.kvproto.TikvGrpc.TikvBlockingStub;
import org.tikv.kvproto.TikvGrpc.TikvStub;

/**
 * Since v4.0.0 TiDB write will not block read (update MinCommitTS). Since v5.0.0 TiDB supports
 * Async-Commit.
 */
public class LockResolverClientV4 extends AbstractRegionStoreClient
    implements AbstractLockResolverClient {
  private static final Logger logger = LoggerFactory.getLogger(LockResolverClientV4.class);

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

  public LockResolverClientV4(
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
    return "V4";
  }

  @Override
  public ResolveLockResult resolveLocks(
      BackOffer bo, long callerStartTS, List<Lock> locks, boolean forWrite) {
    TxnExpireTime msBeforeTxnExpired = new TxnExpireTime();

    if (locks.isEmpty()) {
      return new ResolveLockResult(msBeforeTxnExpired.value());
    }

    Map<Long, Set<RegionVerID>> cleanTxns = new HashMap<>();
    boolean pushFail = false;
    Set<Long> pushed = new HashSet<>(locks.size());

    for (Lock l : locks) {
      TxnStatus status = getTxnStatusFromLock(bo, l, callerStartTS);

      if (status.getTtl() == 0) {
        Set<RegionVerID> cleanRegion =
            cleanTxns.computeIfAbsent(l.getTxnID(), k -> new HashSet<>());

        if (status.getPrimaryLock() != null && status.getPrimaryLock().getUseAsyncCommit()) {
          resolveLockAsync(bo, l, status);
        } else if (l.getLockType() == org.tikv.kvproto.Kvrpcpb.Op.PessimisticLock) {
          resolvePessimisticLock(bo, l, cleanRegion);
        } else {
          resolveLock(bo, l, status, cleanRegion);
        }

      } else {
        long msBeforeLockExpired = TsoUtils.untilExpired(l.getTxnID(), status.getTtl());
        msBeforeTxnExpired.update(msBeforeLockExpired);

        if (forWrite) {
          // Write conflict detected!
          // If it's a optimistic conflict and current txn is earlier than the lock owner,
          // abort current transaction.
          // This could avoids the deadlock scene of two large transaction.
          if (l.getLockType() != org.tikv.kvproto.Kvrpcpb.Op.PessimisticLock
              && l.getTxnID() > callerStartTS) {
            throw new WriteConflictException(
                callerStartTS, l.getTxnID(), status.getCommitTS(), l.getKey().toByteArray());
          }
        } else {
          if (status.getAction() != org.tikv.kvproto.Kvrpcpb.Action.MinCommitTSPushed) {
            pushFail = true;
          } else {
            pushed.add(l.getTxnID());
          }
        }
      }
    }

    if (pushFail) {
      pushed = new HashSet<>();
    }

    return new ResolveLockResult(msBeforeTxnExpired.value(), pushed);
  }

  private void resolvePessimisticLock(BackOffer bo, Lock lock, Set<RegionVerID> cleanRegion) {
    while (true) {
      region = regionManager.getRegionByKey(lock.getKey());

      if (cleanRegion.contains(region.getVerID())) {
        return;
      }

      final long forUpdateTS =
          lock.getLockForUpdateTs() == 0L ? Long.MAX_VALUE : lock.getLockForUpdateTs();

      Supplier<Kvrpcpb.PessimisticRollbackRequest> factory =
          () ->
              Kvrpcpb.PessimisticRollbackRequest.newBuilder()
                  .setContext(region.getContext())
                  .setStartVersion(lock.getTxnID())
                  .setForUpdateTs(forUpdateTS)
                  .addKeys(lock.getKey())
                  .build();

      KVErrorHandler<Kvrpcpb.PessimisticRollbackResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              this,
              region,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null,
              resp -> resp.getErrorsCount() > 0 ? resp.getErrorsList().get(0) : null,
              resolveLockResult -> null,
              0L,
              false);
      Kvrpcpb.PessimisticRollbackResponse resp =
          callWithRetry(bo, TikvGrpc.getKVPessimisticRollbackMethod(), factory, handler);

      if (resp == null) {
        logger.error("getKVPessimisticRollbackMethod failed without a cause");
        regionManager.onRequestFail(region);
        bo.doBackOff(
            BoRegionMiss,
            new TiClientInternalException("getKVPessimisticRollbackMethod failed without a cause"));
        continue;
      }

      if (resp.hasRegionError()) {
        bo.doBackOff(BoRegionMiss, new RegionException(resp.getRegionError()));
        continue;
      }

      if (resp.getErrorsCount() > 0) {
        logger.error(
            String.format(
                "unexpected resolveLock err: %s, lock: %s", resp.getErrorsList().get(0), lock));
        throw new KeyException(resp.getErrorsList().get(0));
      }
    }
  }

  private TxnStatus getTxnStatusFromLock(BackOffer bo, Lock lock, long callerStartTS) {
    long currentTS;

    if (lock.isUseAsyncCommit()) {
      // Async commit doesn't need the current ts since it uses the minCommitTS.
      currentTS = 0;
      // Set to 0 so as not to push forward min commit ts.
      callerStartTS = 0;
    } else if (lock.getTtl() == 0) {
      // NOTE: l.TTL = 0 is a special protocol!!!
      // When the pessimistic txn prewrite meets locks of a txn, it should resolve the lock
      // **unconditionally**.
      // In this case, TiKV use lock TTL = 0 to notify TiDB, and TiDB should resolve the lock!
      // Set currentTS to max uint64 to make the lock expired.
      currentTS = Long.MAX_VALUE;
    } else {
      currentTS = pdClient.getTimestamp(bo).getVersion();
    }

    boolean rollbackIfNotExist = false;
    while (true) {
      try {
        return getTxnStatus(
            bo,
            lock.getTxnID(),
            lock.getPrimary(),
            callerStartTS,
            currentTS,
            rollbackIfNotExist,
            lock);
      } catch (TxnNotFoundException e) {
        // If the error is something other than txnNotFoundErr, throw the error (network
        // unavailable, tikv down, backoff timeout etc) to the caller.
        logger.warn("getTxnStatus error!", e);

        // Handle txnNotFound error.
        // getTxnStatus() returns it when the secondary locks exist while the primary lock doesn't.
        // This is likely to happen in the concurrently prewrite when secondary regions
        // success before the primary region.
        try {
          bo.doBackOff(BoTxnNotFound, e);
        } catch (Throwable ignored) {
        }
      }

      if (TsoUtils.untilExpired(lock.getTxnID(), lock.getTtl()) <= 0) {
        logger.warn(
            String.format(
                "lock txn not found, lock has expired, CallerStartTs=%d lock str=%s",
                callerStartTS, lock.toString()));
        if (lock.getLockType() == Kvrpcpb.Op.PessimisticLock) {
          return new TxnStatus();
        }
        rollbackIfNotExist = true;
      } else {
        if (lock.getLockType() == Kvrpcpb.Op.PessimisticLock) {
          return new TxnStatus(lock.getTtl());
        }
      }
    }
  }

  /**
   * getTxnStatus sends the CheckTxnStatus request to the TiKV server. When rollbackIfNotExist is
   * false, the caller should be careful with the TxnNotFoundException error.
   */
  private TxnStatus getTxnStatus(
      BackOffer bo,
      Long txnID,
      ByteString primary,
      Long callerStartTS,
      Long currentTS,
      boolean rollbackIfNotExist,
      Lock lock) {
    TxnStatus status = getResolved(txnID);
    if (status != null) {
      return status;
    }

    // CheckTxnStatus may meet the following cases:
    // 1. LOCK
    // 1.1 Lock expired -- orphan lock, fail to update TTL, crash recovery etc.
    // 1.2 Lock TTL -- active transaction holding the lock.
    // 2. NO LOCK
    // 2.1 Txn Committed
    // 2.2 Txn Rollbacked -- rollback itself, rollback by others, GC tomb etc.
    // 2.3 No lock -- pessimistic lock rollback, concurrence prewrite.
    Supplier<Kvrpcpb.CheckTxnStatusRequest> factory =
        () -> {
          TiRegion primaryKeyRegion = regionManager.getRegionByKey(primary);
          return Kvrpcpb.CheckTxnStatusRequest.newBuilder()
              .setContext(primaryKeyRegion.getContext())
              .setPrimaryKey(primary)
              .setLockTs(txnID)
              .setCallerStartTs(callerStartTS)
              .setCurrentTs(currentTS)
              .setRollbackIfNotExist(rollbackIfNotExist)
              .build();
        };

    while (true) {
      TiRegion primaryKeyRegion = regionManager.getRegionByKey(primary);
      // new RegionStoreClient for PrimaryKey
      RegionStoreClient primaryKeyRegionStoreClient = clientBuilder.build(primary);
      KVErrorHandler<Kvrpcpb.CheckTxnStatusResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              primaryKeyRegionStoreClient,
              primaryKeyRegionStoreClient.lockResolverClient,
              primaryKeyRegion,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null,
              resp -> resp.hasError() ? resp.getError() : null,
              resolveLockResult -> null,
              callerStartTS,
              false);

      Kvrpcpb.CheckTxnStatusResponse resp =
          primaryKeyRegionStoreClient.callWithRetry(
              bo, TikvGrpc.getKvCheckTxnStatusMethod(), factory, handler);

      if (resp == null) {
        logger.error("getKvCheckTxnStatusMethod failed without a cause");
        regionManager.onRequestFail(primaryKeyRegion);
        bo.doBackOff(
            BoRegionMiss,
            new TiClientInternalException("getKvCheckTxnStatusMethod failed without a cause"));
        continue;
      }

      if (resp.hasRegionError()) {
        bo.doBackOff(BoRegionMiss, new RegionException(resp.getRegionError()));
        continue;
      }

      if (resp.hasError()) {
        Kvrpcpb.KeyError keyError = resp.getError();

        if (keyError.hasTxnNotFound()) {
          throw new TxnNotFoundException();
        }

        logger.error(String.format("unexpected cleanup err: %s, tid: %d", keyError, txnID));
        throw new KeyException(keyError);
      }

      status = new TxnStatus();
      status.setAction(resp.getAction());
      status.setPrimaryLock(resp.getLockInfo());

      if (status.getPrimaryLock() != null && status.getPrimaryLock().getUseAsyncCommit()) {
        if (!TsoUtils.isExpired(txnID, resp.getLockTtl())) {
          status.setTtl(resp.getLockTtl());
        }
      } else if (resp.getLockTtl() != 0) {
        status.setTtl(resp.getLockTtl());
      } else {
        status.setCommitTS(resp.getCommitVersion());

        // If the transaction is still valid with ttl greater than zero, do nothing.
        // If its status is certain:
        //     If transaction is already committed, the result could be cached.
        //     Otherwise:
        //       If l.LockType is pessimistic lock type:
        //           - if its primary lock is pessimistic too, the check txn status result should
        // not be cached.
        //           - if its primary lock is prewrite lock type, the check txn status could be
        // cached, todo.
        //       If l.lockType is prewrite lock type:
        //           - always cache the check txn status result.
        // For prewrite locks, their primary keys should ALWAYS be the correct one and will NOT
        // change.
        if (status.isCommitted()
            || (lock != null
                && lock.getLockType() != org.tikv.kvproto.Kvrpcpb.Op.PessimisticLock)) {
          saveResolved(txnID, status);
        }
      }

      return status;
    }
  }

  /** resolveLockAsync resolves lock assuming it was locked using the async commit protocol. */
  private void resolveLockAsync(BackOffer bo, Lock lock, TxnStatus status) {
    AsyncResolveData resolveData = checkAllSecondaries(bo, lock, status);

    status.setCommitTS(resolveData.getCommitTs());

    resolveData.appendKey(lock.getPrimary());

    GroupKeyResult groupResult =
        Utils.groupKeysByRegion(this.regionManager, resolveData.getKeys(), bo);

    logger.info(
        String.format(
            "resolve async commit, startTS=%d, commitTS=%d",
            lock.getTxnID(), status.getCommitTS()));

    ExecutorService executorService =
        Executors.newFixedThreadPool(
            conf.getKvClientConcurrency(), new ThreadFactoryBuilder().setDaemon(true).build());
    ExecutorCompletionService<Boolean> completionService =
        new ExecutorCompletionService<>(executorService);

    for (Map.Entry<Pair<TiRegion, Metapb.Store>, List<ByteString>> entry :
        groupResult.getGroupsResult().entrySet()) {
      TiRegion tiRegion = entry.getKey().first;
      List<ByteString> keys = entry.getValue();
      completionService.submit(() -> resolveRegionLocks(bo, lock, tiRegion, keys, status));
    }

    try {
      for (int i = 0; i < groupResult.getGroupsResult().size(); i++) {
        completionService.take().get();
      }
    } catch (InterruptedException e) {
      logger.info("async commit recovery (sending ResolveLock) finished with errors", e);
      Thread.currentThread().interrupt();
      throw new TiKVException("Current thread interrupted.", e);
    } catch (ExecutionException e) {
      logger.info("async commit recovery (sending ResolveLock) finished with errors", e);
      throw new TiKVException("Execution exception met.", e);
    } catch (Throwable e) {
      logger.info("async commit recovery (sending ResolveLock) finished with errors", e);
      throw e;
    } finally {
      executorService.shutdownNow();
    }
  }

  /**
   * checkAllSecondaries checks the secondary locks of an async commit transaction to find out the
   * final status of the transaction
   */
  private AsyncResolveData checkAllSecondaries(BackOffer bo, Lock lock, TxnStatus status) {
    AsyncResolveData shared =
        new AsyncResolveData(status.getPrimaryLock().getMinCommitTs(), new ArrayList<>(), false);

    GroupKeyResult groupResult =
        Utils.groupKeysByRegion(
            this.regionManager, status.getPrimaryLock().getSecondariesList(), bo);

    ExecutorService executorService =
        Executors.newFixedThreadPool(
            conf.getKvClientConcurrency(), new ThreadFactoryBuilder().setDaemon(true).build());
    ExecutorCompletionService<Boolean> completionService =
        new ExecutorCompletionService<>(executorService);

    for (Map.Entry<Pair<TiRegion, Metapb.Store>, List<ByteString>> entry :
        groupResult.getGroupsResult().entrySet()) {
      TiRegion tiRegion = entry.getKey().first;
      List<ByteString> keys = entry.getValue();
      completionService.submit(() -> checkSecondaries(bo, lock.getTxnID(), keys, tiRegion, shared));
    }

    try {
      for (int i = 0; i < groupResult.getGroupsResult().size(); i++) {
        completionService.take().get();
      }
      return shared;
    } catch (InterruptedException e) {
      logger.info("async commit recovery (sending CheckSecondaryLocks) finished with errors", e);
      Thread.currentThread().interrupt();
      throw new TiKVException("Current thread interrupted.", e);
    } catch (ExecutionException e) {
      logger.info("async commit recovery (sending CheckSecondaryLocks) finished with errors", e);
      throw new TiKVException("Execution exception met.", e);
    } catch (Throwable e) {
      logger.info("async commit recovery (sending CheckSecondaryLocks) finished with errors", e);
      throw e;
    } finally {
      executorService.shutdownNow();
    }
  }

  private boolean checkSecondaries(
      BackOffer bo,
      long txnID,
      List<ByteString> curKeys,
      TiRegion tiRegion,
      AsyncResolveData shared) {
    RegionStoreClient regionStoreClient = clientBuilder.build(tiRegion);

    Supplier<Kvrpcpb.CheckSecondaryLocksRequest> factory =
        () ->
            Kvrpcpb.CheckSecondaryLocksRequest.newBuilder()
                .setContext(tiRegion.getContext())
                .setStartVersion(txnID)
                .addAllKeys(curKeys)
                .build();

    KVErrorHandler<Kvrpcpb.CheckSecondaryLocksResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            regionStoreClient,
            regionStoreClient.lockResolverClient,
            tiRegion,
            resp -> null,
            resp -> null,
            resolveLockResult -> null,
            0L,
            false);

    Kvrpcpb.CheckSecondaryLocksResponse resp =
        regionStoreClient.callWithRetry(
            bo, TikvGrpc.getKvCheckSecondaryLocksMethod(), factory, handler);

    if (resp == null) {
      logger.error("getKvCheckSecondaryLocksMethod failed without a cause");
      regionManager.onRequestFail(tiRegion);
      bo.doBackOff(
          BoRegionMiss,
          new TiClientInternalException("getKvCheckSecondaryLocksMethod failed without a cause"));

      logger.debug(
          String.format(
              "checkSecondaries: region error, regrouping, txnID=%d, regionId=%d",
              txnID, tiRegion.getId()));
      // If regions have changed, then we might need to regroup the keys. Since this should be rare
      // and for the sake of simplicity, we will resolve regions sequentially.
      GroupKeyResult groupResult = Utils.groupKeysByRegion(this.regionManager, curKeys, bo);
      for (Map.Entry<Pair<TiRegion, Metapb.Store>, List<ByteString>> entry :
          groupResult.getGroupsResult().entrySet()) {
        TiRegion region = entry.getKey().first;
        List<ByteString> keys = entry.getValue();
        checkSecondaries(bo, txnID, keys, region, shared);
      }
    }

    shared.addKeys(resp.getLocksList(), curKeys.size(), txnID, resp.getCommitTs());
    return true;
  }

  /**
   * resolveRegionLocks is essentially the same as resolveLock, but we resolve all keys in the same
   * region at the same time.
   */
  private boolean resolveRegionLocks(
      BackOffer bo, Lock lock, TiRegion tiRegion, List<ByteString> keys, TxnStatus status) {
    RegionStoreClient regionStoreClient = clientBuilder.build(tiRegion);

    Supplier<Kvrpcpb.ResolveLockRequest> factory =
        () ->
            Kvrpcpb.ResolveLockRequest.newBuilder()
                .setContext(tiRegion.getContext())
                .setStartVersion(lock.getTxnID())
                .setCommitVersion(status.getCommitTS())
                .addAllKeys(keys)
                .build();

    KVErrorHandler<Kvrpcpb.ResolveLockResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            regionStoreClient,
            regionStoreClient.lockResolverClient,
            tiRegion,
            resp -> null,
            resp -> null,
            resolveLockResult -> null,
            0L,
            false);

    Kvrpcpb.ResolveLockResponse resp =
        regionStoreClient.callWithRetry(bo, TikvGrpc.getKvResolveLockMethod(), factory, handler);

    if (resp == null || resp.hasRegionError()) {
      logger.error("getKvResolveLockMethod failed without a cause");
      regionManager.onRequestFail(tiRegion);
      bo.doBackOff(
          BoRegionMiss,
          new TiClientInternalException("getKvResolveLockMethod failed without a cause"));

      logger.debug(
          String.format(
              "resolveRegionLocks region error, regrouping lock=%s region=%d",
              lock, tiRegion.getId()));

      // Regroup locks.
      GroupKeyResult groupResult = Utils.groupKeysByRegion(this.regionManager, keys, bo);
      for (Map.Entry<Pair<TiRegion, Metapb.Store>, List<ByteString>> entry :
          groupResult.getGroupsResult().entrySet()) {
        TiRegion region = entry.getKey().first;
        resolveRegionLocks(bo, lock, region, entry.getValue(), status);
      }
    } else if (resp.hasError()) {
      logger.error(
          String.format("unexpected resolveLock err: %s, lock: %s", resp.getError(), lock));
      throw new KeyException(resp.getError());
    }
    return true;
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
