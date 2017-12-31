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
import com.pingcap.tikv.exception.GrpcRegionStaleException;
import com.pingcap.tikv.kvproto.Errorpb;
import com.pingcap.tikv.region.RegionErrorReceiver;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.log4j.Logger;

import java.util.function.Function;

// TODO: consider refactor to Builder mode
public class KVErrorHandler<RespT> implements ErrorHandler<RespT> {
  private static final Logger logger = Logger.getLogger(KVErrorHandler.class);
  private final Function<RespT, Errorpb.Error> getRegionError;
  private final Function<RespT, String> getOtherError;
  private final Function<CacheInvalidateEvent, Void> cacheInvalidateCallBack;
  private final RegionManager regionManager;
  private final RegionErrorReceiver recv;
  private final TiRegion ctxRegion;

  public KVErrorHandler(
      RegionManager regionManager,
      RegionErrorReceiver recv,
      TiRegion ctxRegion,
      Function<RespT, Errorpb.Error> getRegionError,
      Function<RespT, String> getOtherError) {
    this.ctxRegion = ctxRegion;
    this.recv = recv;
    this.regionManager = regionManager;
    this.getRegionError = getRegionError;
    this.getOtherError = getOtherError;
    this.cacheInvalidateCallBack =
        regionManager != null && regionManager.getSession() != null ?
            regionManager.getSession().getCacheInvalidateCallback() : null;
  }

  public KVErrorHandler(
      RegionManager regionManager,
      RegionErrorReceiver recv,
      TiRegion ctxRegion,
      Function<RespT, Errorpb.Error> getRegionError) {
    this(regionManager, recv, ctxRegion, getRegionError, null);
  }

  public void handle(RespT resp) {
    // if resp is null, then region maybe out of dated. we need handle this on RegionManager.
    if (resp == null) {
      logger.warn(String.format("Request Failed with unknown reason for region region [%s]", ctxRegion));
      regionManager.onRequestFail(ctxRegion.getId(), ctxRegion.getLeader().getStoreId());
      notifyCacheInvalidation(
          ctxRegion.getId(),
          ctxRegion.getLeader().getStoreId(),
          CacheInvalidateEvent.CacheType.REQ_FAILED
      );
      return;
    }

    // Region error handling logic
    Errorpb.Error error = getRegionError.apply(resp);
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
        throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      } else if (error.hasStoreNotMatch()) {
        logger.warn(String.format("Store Not Match happened with region id %d, store id %d",
            ctxRegion.getId(),
            ctxRegion.getLeader().getStoreId()));

        regionManager.invalidateRegion(ctxRegion.getId());
        regionManager.invalidateStore(ctxRegion.getLeader().getStoreId());
        notifyCacheInvalidation(
            ctxRegion.getId(),
            ctxRegion.getLeader().getStoreId(),
            CacheInvalidateEvent.CacheType.REGION_STORE
        );
        recv.onStoreNotMatch();
        throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      } else if (error.hasStaleEpoch()) {
        logger.warn(String.format("Stale Epoch encountered for region [%s]", ctxRegion.getId()));
        this.regionManager.onRegionStale(ctxRegion.getId());
        throw new GrpcRegionStaleException(error.toString());
      } else if (error.hasServerIsBusy()) {
        logger.warn(String.format("Server is busy for region [%s]", ctxRegion));
        throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      } else if (error.hasStaleCommand()) {
        logger.warn(String.format("Stale command for region [%s]", ctxRegion));
        throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      } else if (error.hasRaftEntryTooLarge()) {
        logger.warn(String.format("Raft too large for region [%s]", ctxRegion));
        throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      } else if (error.hasKeyNotInRegion()) {
        ByteString invalidKey = error.getKeyNotInRegion().getKey();
        logger.warn(String.format("Key not in region [%s] for key [%s]", ctxRegion, KeyUtils.formatBytes(invalidKey)));
        throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      } else {
        logger.warn(String.format("Unknown error for region [%s]", error));
        // for other errors, we only drop cache here and throw a retryable exception.
        regionManager.invalidateRegion(ctxRegion.getId());
        regionManager.invalidateStore(ctxRegion.getLeader().getStoreId());
        notifyCacheInvalidation(
            ctxRegion.getId(),
            ctxRegion.getLeader().getStoreId(),
            CacheInvalidateEvent.CacheType.REGION_STORE
        );
        throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      }
    }

    // Other error handling logic
    // Currently we need to handle potential other errors from coprocessor responses.
    if (getOtherError != null) {
      String otherError = getOtherError.apply(resp);
      if (otherError != null &&
          !otherError.trim().isEmpty()) {
        logger.warn(String.format("Other error occurred for region [%s]", ctxRegion));
        // Just throw to upper layer to handle
        throw new RuntimeException("Received other error from TiKV:" + otherError);
      }
    }
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
}
