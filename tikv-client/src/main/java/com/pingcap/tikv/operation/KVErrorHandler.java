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

import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.event.CacheInvalidateEvent;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.region.RegionErrorReceiver;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffFunction;
import com.pingcap.tikv.util.BackOffer;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Errorpb;

// TODO: consider refactor to Builder mode
public class KVErrorHandler<RespT> implements ErrorHandler<RespT> {
  private static final Logger logger = LoggerFactory.getLogger(KVErrorHandler.class);
  // if a store does not have leader currently, store id is set to 0
  private static final int NO_LEADER_STORE_ID = 0;
  private final Function<RespT, Errorpb.Error> getRegionError;
  private final Function<CacheInvalidateEvent, Void> cacheInvalidateCallBack;
  private final RegionManager regionManager;
  private final RegionErrorReceiver recv;

  public KVErrorHandler(
      RegionManager regionManager,
      RegionErrorReceiver recv,
      Function<RespT, Errorpb.Error> getRegionError) {
    this.recv = recv;
    this.regionManager = regionManager;
    this.getRegionError = getRegionError;
    this.cacheInvalidateCallBack =
        regionManager != null ? regionManager.getCacheInvalidateCallback() : null;
  }

  private Errorpb.Error getRegionError(RespT resp) {
    if (getRegionError != null) {
      return getRegionError.apply(resp);
    }
    return null;
  }

  private void invalidateRegionStoreCache(TiRegion ctxRegion) {
    regionManager.invalidateRegion(ctxRegion);
    regionManager.invalidateStore(ctxRegion.getLeader().getStoreId());
    notifyRegionStoreCacheInvalidate(
      ctxRegion.getId(),
      ctxRegion.getLeader().getStoreId(),
      CacheInvalidateEvent.CacheType.REGION_STORE);
  }

  /** Used for notifying Spark driver to invalidate cache from Spark workers. */
  private void notifyRegionStoreCacheInvalidate(
      long regionId, long storeId, CacheInvalidateEvent.CacheType type) {
    if (cacheInvalidateCallBack != null) {
      cacheInvalidateCallBack.apply(new CacheInvalidateEvent(regionId, storeId, true, true, type));
      logger.info(
          "Accumulating cache invalidation info to driver:regionId="
              + regionId
              + ",storeId="
              + storeId
              + ",type="
              + type.name());
    } else {
      logger.warn(
          "Failed to send notification back to driver since CacheInvalidateCallBack is null in executor node.");
    }
  }

  private void notifyRegionCacheInvalidate(long regionId) {
    if (cacheInvalidateCallBack != null) {
      cacheInvalidateCallBack.apply(
          new CacheInvalidateEvent(
              regionId, 0, true, false, CacheInvalidateEvent.CacheType.REGION_STORE));
      logger.info(
          "Accumulating cache invalidation info to driver:regionId="
              + regionId
              + ",type="
              + CacheInvalidateEvent.CacheType.REGION_STORE.name());
    } else {
      logger.warn(
          "Failed to send notification back to driver since CacheInvalidateCallBack is null in executor node.");
    }
  }

  private void notifyStoreCacheInvalidate(long storeId) {
    if (cacheInvalidateCallBack != null) {
      cacheInvalidateCallBack.apply(
          new CacheInvalidateEvent(
              0, storeId, false, true, CacheInvalidateEvent.CacheType.REGION_STORE));
    } else {
      logger.warn(
          "Failed to send notification back to driver since CacheInvalidateCallBack is null in executor node.");
    }
  }

  // Referenced from TiDB
  // store/tikv/region_request.go - onRegionError

  /** @return true: client should retry */
  @Override
  public boolean handleResponseError(BackOffer backOffer, RespT resp) {
    TiRegion region = recv.getRegion();
    if (resp == null) {
      String msg =
          String.format("Request Failed with unknown reason for region region [%s]", region);
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
                region.getId(), region.getLeader().getStoreId(), newStoreId));

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
          TiRegion newRegion = this.regionManager.updateLeader(region, newStoreId);
          retry =
              newRegion != null
                  && recv.onNotLeader(this.regionManager.getStoreById(newStoreId), newRegion);

          backOffFuncType = BackOffFunction.BackOffFuncType.BoUpdateLeader;
        } else {
          logger.info(
              String.format(
                  "Received zero store id, from region %d try next time", region.getId()));

          backOffFuncType = BackOffFunction.BackOffFuncType.BoRegionMiss;
          retry = false;
        }

        if (!retry) {
          this.regionManager.invalidateRegion(region);
          notifyRegionCacheInvalidate(region.getId());
        }

        backOffer.doBackOff(backOffFuncType, new GrpcException(error.toString()));

        return retry;
      } else if (error.hasStoreNotMatch()) {
        // this error is reported from raftstore:
        // store_id requested at the moment is inconsistent with that expected
        // Solution：re-fetch from PD
        long storeId = region.getLeader().getStoreId();
        long actualStoreId = error.getStoreNotMatch().getActualStoreId();
        logger.warn(
            String.format(
                "Store Not Match happened with region id %d, store id %d, actual store id %d",
                region.getId(), storeId, actualStoreId));

        invalidateRegionStoreCache(region);
        // recv.onStoreNotMatch(this.regionManager.getStoreById(storeId));
        // assume this is a low probability error, do not retry, just re-split the request by
        // throwing it out.
        return false;
      } else if (error.hasEpochNotMatch()) {
        // this error is reported from raftstore:
        // region has outdated version，please try later.
        logger.warn(String.format("Stale Epoch encountered for region [%s]", region));
        this.regionManager.onRegionStale(region);
        notifyRegionCacheInvalidate(region.getId());
        return false;
      } else if (error.hasServerIsBusy()) {
        // this error is reported from kv:
        // will occur when write pressure is high. Please try later.
        logger.warn(
            String.format(
                "Server is busy for region [%s], reason: %s",
                region, error.getServerIsBusy().getReason()));
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoServerBusy,
            new StatusRuntimeException(
                Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString())));
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(error.getMessage()));
        return true;
      } else if (error.hasStaleCommand()) {
        // this error is reported from raftstore:
        // command outdated, please try later
        logger.warn(String.format("Stale command for region [%s]", region));
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(error.getMessage()));
        return true;
      } else if (error.hasRaftEntryTooLarge()) {
        logger.warn(String.format("Raft too large for region [%s]", region));
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
                region, KeyUtils.formatBytesUTF8(invalidKey)));
        throw new StatusRuntimeException(Status.UNKNOWN.withDescription(error.toString()));
      }

      logger.warn(String.format("Unknown error %s for region [%s]", error, region));
      // For other errors, we only drop cache here.
      // Upper level may split this task.
      invalidateRegionStoreCache(region);
      // retry if raft proposal is dropped, it indicates the store is in the middle of transition
      if (error.getMessage().contains("Raft ProposalDropped")) {
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(error.getMessage()));
        return true;
      }
    }

    return false;
  }

  @Override
  public boolean handleRequestError(BackOffer backOffer, Exception e) {
    TiRegion region = recv.getRegion();
    regionManager.onRequestFail(region);
    notifyRegionStoreCacheInvalidate(
        region.getId(), region.getLeader().getStoreId(), CacheInvalidateEvent.CacheType.REQ_FAILED);

    backOffer.doBackOff(
        BackOffFunction.BackOffFuncType.BoTiKVRPC,
        new GrpcException(
            "send tikv request error: " + e.getMessage() + ", try next peer later", e));
    // TiKV maybe down, so do not retry in `callWithRetry`
    // should re-fetch the new leader from PD and send request to it
    return false;
  }
}
