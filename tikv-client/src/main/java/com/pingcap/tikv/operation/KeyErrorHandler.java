package com.pingcap.tikv.operation;

import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoTxnLockFast;

import com.pingcap.tikv.event.CacheInvalidateEvent;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.region.RegionErrorReceiver;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.txn.Lock;
import com.pingcap.tikv.txn.LockResolverClient;
import com.pingcap.tikv.util.BackOffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.tikv.kvproto.Kvrpcpb.KvPair;

public class KeyErrorHandler<RespT> implements ErrorHandler<RespT> {

  private final RegionManager regionManager;
  private final RegionErrorReceiver recv;
  private final TiRegion ctxRegion;
  Function<RespT, List<KvPair>> getKeyError;
  private final Function<CacheInvalidateEvent, Void> cacheInvalidateCallBack;
  private final LockResolverClient lockResolverClient;

  public KeyErrorHandler(
      RegionManager regionManager,
      RegionErrorReceiver recv,
      TiRegion ctxRegion,
      Function<RespT, List<KvPair>> getKeyError,
      LockResolverClient lockResolverClient) {
    this.ctxRegion = ctxRegion;
    this.recv = recv;
    this.regionManager = regionManager;
    this.getKeyError = getKeyError;
    this.cacheInvalidateCallBack =
        regionManager != null ? regionManager.getCacheInvalidateCallback() : null;
    this.lockResolverClient = lockResolverClient;
  }

  @Override
  public boolean handleResponseError(BackOffer backOffer, RespT resp) {
    List<Lock> locks = new ArrayList<>();
    List<KvPair> kvPairs = getKeyError.apply(resp);
    for (KvPair pair : kvPairs) {
      if (pair.hasError()) {
        if (pair.getError().hasLocked()) {
          Lock lock = new Lock(pair.getError().getLocked());
          locks.add(lock);
        } else {
          throw new KeyException(pair.getError());
        }
      }
    }

    if (!locks.isEmpty()) {
      boolean ok = lockResolverClient.resolveLocks(backOffer, locks);
      if (!ok) {
        //         if not resolve all locks, we wait and retry
        backOffer.doBackOff(BoTxnLockFast, new KeyException((kvPairs.get(0).getError())));
      }
      //
      //       TODO: we should retry
      //       fix me
    }
    //    if (resp.hasRegionError()) {
    //      throw new RegionException(resp.getRegionError());
    //    }
    //    return resp.getPairsList();
    return false;
  }

  @Override
  public boolean handleRequestError(BackOffer backOffer, Exception e) {
    return false;
  }
}
