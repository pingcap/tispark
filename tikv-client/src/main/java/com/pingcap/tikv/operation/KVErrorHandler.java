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
import com.pingcap.tikv.kvproto.Errorpb;
import com.pingcap.tikv.region.RegionErrorReceiver;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.BackOffFunction;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.log4j.Logger;

import java.util.function.Function;

// TODO: consider refactor to Builder mode
public class KVErrorHandler<RespT> implements ErrorHandler<RespT> {
  private static final Logger logger = Logger.getLogger(KVErrorHandler.class);
  private final Function<RespT, Errorpb.Error> getRegionError;
  private final Function<CacheInvalidateEvent, Void> cacheInvalidateCallBack;
  private final RegionManager regionManager;
  private final RegionErrorReceiver recv;
  private final TiRegion ctxRegion;

  public KVErrorHandler(
      RegionManager regionManager,
      RegionErrorReceiver recv,
      TiRegion ctxRegion,
      Function<RespT, Errorpb.Error> getRegionError) {
    this.ctxRegion = ctxRegion;
    this.recv = recv;
    this.regionManager = regionManager;
    this.getRegionError = getRegionError;
    this.cacheInvalidateCallBack =
        regionManager != null && regionManager.getSession() != null ?
            regionManager.getSession().getCacheInvalidateCallback() : null;
  }

  private Errorpb.Error getRegionError(RespT resp) {
    if (getRegionError != null) {
      return getRegionError.apply(resp);
    }
    return null;
  }

  private void invalidateRegionStoreCache(TiRegion ctxRegion) {
    regionManager.invalidateRegion(ctxRegion.getId());
    regionManager.invalidateStore(ctxRegion.getLeader().getStoreId());
    notifyCacheInvalidation(
        ctxRegion.getId(),
        ctxRegion.getLeader().getStoreId(),
        CacheInvalidateEvent.CacheType.REGION_STORE
    );
  }

  /**
   * Used for notifying Spark driver to invalidate cache from Spark workers.
   */
  private void notifyCacheInvalidation(long regionId, long storeId, CacheInvalidateEvent.CacheType type) {
    if (cacheInvalidateCallBack != null) {
      cacheInvalidateCallBack.apply(new CacheInvalidateEvent(
          regionId, storeId,
          true, true,
          type));
    } else {
      logger.error("Failed to send notification back to driver since CacheInvalidateCallBack is null in executor node.");
    }
  }

  // Referenced from TiDB
  // store/tikv/region_request.go - onRegionError
  @Override
  public boolean handleResponseError(BackOffer backOffer, RespT resp) {
    if (resp == null) {
      String msg = String.format("Request Failed with unknown reason for region region [%s]", ctxRegion);
      logger.warn(msg);
      return handleRequestError(backOffer, new GrpcException(msg));
    }

    // Region error handling logic
    Errorpb.Error error = getRegionError(resp);
    if (error != null) {
      if (error.hasNotLeader()) {
        // update Leader here
        logger.warn(String.format("NotLeader Error with region id %d and store id %d",
            ctxRegion.getId(),
            ctxRegion.getLeader().getStoreId()));

        long newStoreId = error.getNotLeader().getLeader().getStoreId();
        regionManager.updateLeader(ctxRegion.getId(), newStoreId);
        notifyCacheInvalidation(
            ctxRegion.getId(),
            newStoreId,
            CacheInvalidateEvent.CacheType.LEADER
        );
        recv.onNotLeader(this.regionManager.getRegionById(ctxRegion.getId()),
            this.regionManager.getStoreById(newStoreId));

        BackOffFunction.BackOffFuncType backOffFuncType;
        if (error.getNotLeader().getLeader() != null) {
          backOffFuncType = BackOffFunction.BackOffFuncType.BoUpdateLeader;
        } else {
          backOffFuncType = BackOffFunction.BackOffFuncType.BoRegionMiss;
        }
        backOffer.doBackOff(backOffFuncType, new GrpcException(error.toString()));

        return true;
      } else if (error.hasStoreNotMatch()) {
        logger.warn(String.format("Store Not Match happened with region id %d, store id %d",
            ctxRegion.getId(),
            ctxRegion.getLeader().getStoreId()));

        invalidateRegionStoreCache(ctxRegion);
        recv.onStoreNotMatch();
        return true;
      } else if (error.hasStaleEpoch()) {
        logger.warn(String.format("Stale Epoch encountered for region [%s]", ctxRegion));
        this.regionManager.onRegionStale(ctxRegion.getId());
        throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      } else if (error.hasServerIsBusy()) {
        logger.warn(String.format("Server is busy for region [%s], reason: %s", ctxRegion, error.getServerIsBusy().getReason()));
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoServerBusy,
            new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString())));
        return true;
      } else if (error.hasStaleCommand()) {
        logger.warn(String.format("Stale command for region [%s]", ctxRegion));
        return true;
      } else if (error.hasRaftEntryTooLarge()) {
        logger.warn(String.format("Raft too large for region [%s]", ctxRegion));
        throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      } else if (error.hasKeyNotInRegion()) {
        ByteString invalidKey = error.getKeyNotInRegion().getKey();
        logger.error(String.format("Key not in region [%s] for key [%s], this error should not happen here.", ctxRegion, KeyUtils.formatBytes(invalidKey)));
        throw new StatusRuntimeException(Status.UNKNOWN.withDescription(error.toString()));
      }

      logger.warn(String.format("Unknown error for region [%s]", ctxRegion));
      // For other errors, we only drop cache here.
      // Upper level may split this task.
      invalidateRegionStoreCache(ctxRegion);
    }

    return false;
  }

  @Override
  public boolean handleRequestError(BackOffer backOffer, Exception e) {
    regionManager.onRequestFail(ctxRegion.getId(), ctxRegion.getLeader().getStoreId());
    notifyCacheInvalidation(
        ctxRegion.getId(),
        ctxRegion.getLeader().getStoreId(),
        CacheInvalidateEvent.CacheType.REQ_FAILED
    );

    backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoTiKVRPC,
        new GrpcException("send tikv request error: " + e.getMessage() + ", try next peer later", e));
    return true;
  }
}
