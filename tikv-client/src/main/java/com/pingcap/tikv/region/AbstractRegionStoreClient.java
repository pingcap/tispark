/*
 *
 * Copyright 2019 PingCAP, Inc.
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

package com.pingcap.tikv.region;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.pingcap.tikv.AbstractGRPCClient;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.util.ChannelFactory;
import io.grpc.ManagedChannel;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.TikvGrpc;

public abstract class AbstractRegionStoreClient
    extends AbstractGRPCClient<TikvGrpc.TikvBlockingStub, TikvGrpc.TikvStub>
    implements RegionErrorReceiver {

  protected final RegionManager regionManager;
  protected TiRegion region;

  protected AbstractRegionStoreClient(
      TiConfiguration conf,
      TiRegion region,
      ChannelFactory channelFactory,
      TikvGrpc.TikvBlockingStub blockingStub,
      TikvGrpc.TikvStub asyncStub,
      RegionManager regionManager) {
    super(conf, channelFactory, blockingStub, asyncStub);
    checkNotNull(region, "Region is empty");
    checkNotNull(region.getLeader(), "Leader Peer is null");
    checkArgument(region.getLeader() != null, "Leader Peer is null");
    this.region = region;
    this.regionManager = regionManager;
  }

  public TiRegion getRegion() {
    return region;
  }

  @Override
  protected TikvGrpc.TikvBlockingStub getBlockingStub() {
    return blockingStub.withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
  }

  @Override
  protected TikvGrpc.TikvStub getAsyncStub() {
    return asyncStub.withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
  }

  @Override
  public void close() throws GrpcException {}

  /**
   * onNotLeader deals with NotLeaderError and returns whether re-splitting key range is needed
   *
   * @param newStore the new store presented by NotLeader Error
   * @return false when re-split is needed.
   */
  @Override
  public boolean onNotLeader(Metapb.Store newStore) {
    if (logger.isDebugEnabled()) {
      logger.debug(region + ", new leader = " + newStore.getId());
    }
    TiRegion cachedRegion = regionManager.getRegionByKey(region.getStartKey());
    // When switch leader fails or the region changed its key range,
    // it would be necessary to re-split task's key range for new region.
    if (!region.getStartKey().equals(cachedRegion.getStartKey())
        || !region.getEndKey().equals(cachedRegion.getEndKey())) {
      return false;
    }
    region = cachedRegion;
    String addressStr = regionManager.getStoreById(region.getLeader().getStoreId()).getAddress();
    ManagedChannel channel = channelFactory.getChannel(addressStr);
    blockingStub = TikvGrpc.newBlockingStub(channel);
    asyncStub = TikvGrpc.newStub(channel);
    return true;
  }

  @Override
  public void onStoreNotMatch(Metapb.Store store) {
    String addressStr = store.getAddress();
    ManagedChannel channel = channelFactory.getChannel(addressStr);
    blockingStub = TikvGrpc.newBlockingStub(channel);
    asyncStub = TikvGrpc.newStub(channel);
    if (logger.isDebugEnabled() && region.getLeader().getStoreId() != store.getId()) {
      logger.debug(
          "store_not_match may occur? "
              + region
              + ", original store = "
              + store.getId()
              + " address = "
              + addressStr);
    }
  }
}
