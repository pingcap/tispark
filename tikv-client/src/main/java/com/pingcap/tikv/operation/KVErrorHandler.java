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

package com.pingcap.tikv.operation;

import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoTxnLockFast;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.event.CacheInvalidateEvent;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.region.RegionErrorReceiver;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.txn.AbstractLockResolverClient;
import com.pingcap.tikv.txn.Lock;
import com.pingcap.tikv.txn.ResolveLockResult;
import com.pingcap.tikv.util.BackOffFunction;
import com.pingcap.tikv.util.BackOffer;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.CleanupResponse;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.kvproto.Kvrpcpb.ScanResponse;

// TODO: consider refactor to Builder mode
// TODO: KVErrorHandler should resolve locks if it could.
// TODO: consider refactor to Builder mode
public class KVErrorHandler<RespT> implements ErrorHandler<RespT> {
  private static final Logger logger = LoggerFactory.getLogger(KVErrorHandler.class);
  // if a store does not have leader currently, store id is set to 0
  private static final int NO_LEADER_STORE_ID = 0;
  private final Function<RespT, Errorpb.Error> getRegionError;
  private final Function<RespT, Kvrpcpb.KeyError> getKeyError;
  private final Function<CacheInvalidateEvent, Void> cacheInvalidateCallBack;
  private final Function<ResolveLockResult, Object> resolveLockResultCallback;
  private final RegionManager regionManager;
  private final RegionErrorReceiver recv;
  private final AbstractLockResolverClient lockResolverClient;
  private final long callerStartTS;
  private final boolean forWrite;

  public KVErrorHandler(
      RegionManager regionManager,
      RegionErrorReceiver recv,
      AbstractLockResolverClient lockResolverClient,
      Function<RespT, Errorpb.Error> getRegionError,
      Function<RespT, Kvrpcpb.KeyError> getKeyError,
      Function<ResolveLockResult, Object> resolveLockResultCallback,
      long callerStartTS,
      boolean forWrite) {
    this.recv = recv;
    this.lockResolverClient = lockResolverClient;
    this.regionManager = regionManager;
    this.getRegionError = getRegionError;
    this.getKeyError = getKeyError;
    this.cacheInvalidateCallBack =
        regionManager != null ? regionManager.getCacheInvalidateCallback() : null;
    this.resolveLockResultCallback = resolveLockResultCallback;
    this.callerStartTS = callerStartTS;
    this.forWrite = forWrite;
  }

  public KVErrorHandler(
      RegionManager regionManager,
      RegionErrorReceiver recv,
      Function<RespT, Errorpb.Error> getRegionError) {
    this.recv = recv;
    this.lockResolverClient = null;
    this.regionManager = regionManager;
    this.getRegionError = getRegionError;
    this.getKeyError = resp -> null;
    this.cacheInvalidateCallBack =
        regionManager != null ? regionManager.getCacheInvalidateCallback() : null;
    this.resolveLockResultCallback = resolveLock -> null;
    this.callerStartTS = 0;
    this.forWrite = false;
  }

  private Errorpb.Error getRegionError(RespT resp) {
    if (getRegionError != null) {
      return getRegionError.apply(resp);
    }
    return null;
  }

  private void invalidateRegionStoreCache(TiRegion region) {
    regionManager.invalidateRegion(region);
    regionManager.invalidateStore(region.getLeader().getStoreId());
    notifyRegionStoreCacheInvalidate(region, CacheInvalidateEvent.CacheType.REGION_STORE);
  }

  private void notifyRegionStoreCacheInvalidate(
      TiRegion region, CacheInvalidateEvent.CacheType type) {
    if (cacheInvalidateCallBack != null) {
      cacheInvalidateCallBack.apply(new CacheInvalidateEvent(region, true, true, type));
      logger.info(
          "Accumulating cache invalidation info to driver:regionId="
              + region.getId()
              + ",storeId="
              + region.getLeader().getStoreId()
              + ",type="
              + type.name());
    } else {
      logger.warn(
          "Failed to send notification back to driver since CacheInvalidateCallBack is null in executor node.");
    }
  }

  private void notifyRegionCacheInvalidate(TiRegion region) {
    if (cacheInvalidateCallBack != null) {
      cacheInvalidateCallBack.apply(
          new CacheInvalidateEvent(
              region, true, false, CacheInvalidateEvent.CacheType.REGION_STORE));
      logger.info(
          "Accumulating cache invalidation info to driver:regionId="
              + region.getId()
              + ",type="
              + CacheInvalidateEvent.CacheType.REGION_STORE.name());
    } else {
      logger.warn(
          "Failed to send notification back to driver since CacheInvalidateCallBack is null in executor node.");
    }
  }

  private void resolveLocks(BackOffer backOffer, List<Lock> locks) {
    if (lockResolverClient != null) {
      logger.warn("resolving " + locks.size() + " locks");

      resolveLock(backOffer, locks.get(0));
    }
  }

  private void resolveLock(BackOffer backOffer, Lock lock) {
    if (lockResolverClient != null) {
      logger.warn("resolving lock");

      ResolveLockResult resolveLockResult =
          lockResolverClient.resolveLocks(
              backOffer, callerStartTS, Collections.singletonList(lock), forWrite);
      resolveLockResultCallback.apply(resolveLockResult);
      long msBeforeExpired = resolveLockResult.getMsBeforeTxnExpired();
      if (msBeforeExpired > 0) {
        // if not resolve all locks, we wait and retry
        backOffer.doBackOffWithMaxSleep(
            BoTxnLockFast, msBeforeExpired, new KeyException(lock.toString()));
      }
    }
  }

  // Referenced from TiDB
  // store/tikv/region_request.go - onRegionError

  /** @return true: client should retry */
  @Override
  public boolean handleResponseError(BackOffer backOffer, RespT resp) {
    if (resp == null) {
      String msg =
          String.format(
              "Request Failed with unknown reason for region region [%s]", recv.getRegion());
      logger.warn(msg);
      return handleRequestError(backOffer, new GrpcException(msg));
    }

    // Region error handling logic
    Errorpb.Error error = getRegionError(resp);
    if (error != null) {
      if (error.hasNotLeader()) {
        // this error is reported from raftstore:
        // peer of current request is not leader, the following might be its causes:
        // 1. cache is outdated, region has changed its leader, can be solved by re-fetching from PD
        // 2. leader of current region is missing, need to wait and then fetch region info from PD
        long newStoreId = error.getNotLeader().getLeader().getStoreId();
        boolean retry;

        // update Leader here
        logger.warn(
            String.format(
                "NotLeader Error with region id %d and store id %d, new store id %d",
                recv.getRegion().getId(), recv.getRegion().getLeader().getStoreId(), newStoreId));

        BackOffFunction.BackOffFuncType backOffFuncType;
        // if there's current no leader, we do not trigger update pd cache logic
        // since issuing store = NO_LEADER_STORE_ID requests to pd will definitely fail.
        if (newStoreId != NO_LEADER_STORE_ID) {
          // If update leader fails, we need to fetch new region info from pd,
          // and re-split key range for new region. Setting retry to false will
          // stop retry and enter handleCopResponse logic, which would use RegionMiss
          // backOff strategy to wait, fetch new region and re-split key range.
          // onNotLeader is only needed when updateLeader succeeds, thus switch
          // to a new store address.
          TiRegion newRegion = this.regionManager.updateLeader(recv.getRegion(), newStoreId);
          retry =
              newRegion != null
                  && recv.onNotLeader(this.regionManager.getStoreById(newStoreId), newRegion);
          if (!retry) {
            notifyRegionStoreCacheInvalidate(
                recv.getRegion(), CacheInvalidateEvent.CacheType.LEADER);
          }

          backOffFuncType = BackOffFunction.BackOffFuncType.BoUpdateLeader;
        } else {
          logger.info(
              String.format(
                  "Received zero store id, from region %d try next time",
                  recv.getRegion().getId()));

          backOffFuncType = BackOffFunction.BackOffFuncType.BoRegionMiss;
          retry = false;
        }

        if (!retry) {
          this.regionManager.invalidateRegion(recv.getRegion());
        }

        backOffer.doBackOff(backOffFuncType, new GrpcException(error.toString()));

        return retry;
      } else if (error.hasStoreNotMatch()) {
        // this error is reported from raftstore:
        // store_id requested at the moment is inconsistent with that expected
        // Solution：re-fetch from PD
        long storeId = recv.getRegion().getLeader().getStoreId();
        long actualStoreId = error.getStoreNotMatch().getActualStoreId();
        logger.warn(
            String.format(
                "Store Not Match happened with region id %d, store id %d, actual store id %d",
                recv.getRegion().getId(), storeId, actualStoreId));

        invalidateRegionStoreCache(recv.getRegion());
        recv.onStoreNotMatch(this.regionManager.getStoreById(storeId));
        // assume this is a low probability error, do not retry, just re-split the request by
        // throwing it out.
        return false;
      } else if (error.hasEpochNotMatch()) {
        // this error is reported from raftstore:
        // region has outdated version，please try later.
        logger.warn(String.format("Stale Epoch encountered for region [%s]", recv.getRegion()));
        this.regionManager.onRegionStale(recv.getRegion());
        notifyRegionCacheInvalidate(recv.getRegion());
        return false;
      } else if (error.hasServerIsBusy()) {
        // this error is reported from kv:
        // will occur when write pressure is high. Please try later.
        logger.warn(
            String.format(
                "Server is busy for region [%s], reason: %s",
                recv.getRegion(), error.getServerIsBusy().getReason()));
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoServerBusy,
            new StatusRuntimeException(
                Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString())));
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(error.getMessage()));
        return true;
      } else if (error.hasRegionNotFound()) {
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(error.getMessage()));
        this.regionManager.onRegionStale(recv.getRegion());
        notifyRegionCacheInvalidate(recv.getRegion());
        return false;
      } else if (error.hasStaleCommand()) {
        // this error is reported from raftstore:
        // command outdated, please try later
        logger.warn(String.format("Stale command for region [%s]", recv.getRegion()));
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(error.getMessage()));
        return true;
      } else if (error.hasRaftEntryTooLarge()) {
        logger.warn(String.format("Raft too large for region [%s]", recv.getRegion()));
        throw new StatusRuntimeException(
            Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      } else if (error.hasKeyNotInRegion()) {
        // this error is reported from raftstore:
        // key requested is not in current region
        // should not happen here.
        ByteString invalidKey = error.getKeyNotInRegion().getKey();
        logger.error(
            String.format(
                "Key not in region [%s] for key [%s], this error should not happen here.",
                recv.getRegion(), KeyUtils.formatBytesUTF8(invalidKey)));
        throw new StatusRuntimeException(Status.UNKNOWN.withDescription(error.toString()));
      }

      logger.warn(String.format("Unknown error %s for region [%s]", error, recv.getRegion()));
      // For other errors, we only drop cache here.
      // Upper level may split this task.
      invalidateRegionStoreCache(recv.getRegion());
      // retry if raft proposal is dropped, it indicates the store is in the middle of transition
      if (error.getMessage().contains("Raft ProposalDropped")
          || error.getMessage().contains("is missing")) {
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(error.getMessage()));
        return true;
      }
    }

    boolean retry = false;

    if (resp instanceof ScanResponse) {
      List<KvPair> kvPairs = ((ScanResponse) resp).getPairsList();
      List<Lock> locks = new ArrayList<>();
      for (KvPair kvPair : kvPairs) {
        if (kvPair.hasError()) {
          Lock lock = AbstractLockResolverClient.extractLockFromKeyErr(kvPair.getError());
          locks.add(lock);
        }
      }
      if (!locks.isEmpty()) {
        try {
          resolveLocks(backOffer, locks);
          retry = true;
        } catch (KeyException e) {
          logger.warn("Unable to handle KeyExceptions other than LockException", e);
        }
      }
    } else if (!(resp instanceof CleanupResponse)) {
      // Key error handling logic
      Kvrpcpb.KeyError keyError = getKeyError.apply(resp);
      if (keyError != null) {
        try {
          Lock lock = AbstractLockResolverClient.extractLockFromKeyErr(keyError);
          resolveLock(backOffer, lock);
          retry = true;
        } catch (KeyException e) {
          logger.warn("Unable to handle KeyExceptions other than LockException", e);
        }
      }
    }
    return retry;
  }

  @Override
  public boolean handleRequestError(BackOffer backOffer, Exception e) {
    regionManager.onRequestFail(recv.getRegion());
    notifyRegionStoreCacheInvalidate(recv.getRegion(), CacheInvalidateEvent.CacheType.REQ_FAILED);

    backOffer.doBackOff(
        BackOffFunction.BackOffFuncType.BoTiKVRPC,
        new GrpcException(
            "send tikv request error: " + e.getMessage() + ", try next peer later", e));
    // TiKV maybe down, so do not retry in `callWithRetry`
    // should re-fetch the new leader from PD and send request to it
    return false;
  }
}
