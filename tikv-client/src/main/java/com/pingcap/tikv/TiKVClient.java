package com.pingcap.tikv;

import com.pingcap.tikv.region.RegionErrorReceiver;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.ChannelFactory;
import io.grpc.ManagedChannel;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.TikvGrpc;
import org.tikv.kvproto.TikvGrpc.TikvBlockingStub;
import org.tikv.kvproto.TikvGrpc.TikvStub;

public class TiKVClient extends AbstractGRPCClient<TikvBlockingStub, TikvStub>
    implements RegionErrorReceiver {

  protected TikvBlockingStub blockingStub;
  protected TikvStub asyncStub;
  protected TiRegion region;
  protected final RegionManager regionManager;

  public TiKVClient(
      TiConfiguration conf,
      TikvBlockingStub blockingStub,
      TikvStub asyncStub,
      ChannelFactory channelFactory,
      RegionManager regionManager) {
    super(conf, channelFactory);
    this.blockingStub = blockingStub;
    this.regionManager = regionManager;
    this.asyncStub = asyncStub;
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
    TiRegion cachedRegion = regionManager.getRegionById(region.getId());
    // When switch leader fails or the region changed its key range,
    // it would be necessary to re-split task's key range for new region.
    if (!region.getStartKey().equals(cachedRegion.getStartKey())
        || !region.getEndKey().equals(cachedRegion.getEndKey())) {
      return false;
    }
    region = cachedRegion;
    String addressStr = newStore.getAddress();
    ManagedChannel channel = channelFactory.getChannel(addressStr);
    blockingStub = TikvGrpc.newBlockingStub(channel);
    asyncStub = TikvGrpc.newStub(channel);
    return true;
  }

  @Override
  public void onStoreNotMatch(Store store) {
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

  @Override
  protected TikvBlockingStub getBlockingStub() {
    return blockingStub.withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
  }

  @Override
  protected TikvStub getAsyncStub() {
    return asyncStub.withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
  }

  @Override
  public void close() throws Exception {}
}
