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

import com.pingcap.tikv.PDClient;
import com.pingcap.tikv.StoreVersion;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.Version;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ChannelFactory;
import java.util.List;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.TikvGrpc;

public interface AbstractLockResolverClient {
  /** ResolvedCacheSize is max number of cached txn status. */
  long RESOLVED_TXN_CACHE_SIZE = 2048;

  /** transaction involves keys exceed this threshold can be treated as `big transaction`. */
  long BIG_TXN_THRESHOLD = 16;

  static Lock extractLockFromKeyErr(Kvrpcpb.KeyError keyError) {
    if (keyError.hasLocked()) {
      return new Lock(keyError.getLocked());
    }

    if (keyError.hasConflict()) {
      Kvrpcpb.WriteConflict conflict = keyError.getConflict();
      throw new KeyException(
          keyError,
          String.format(
              "scan meet key conflict on primary key %s at commit ts %s",
              conflict.getPrimary(), conflict.getConflictTs()));
    }

    if (!keyError.getRetryable().isEmpty()) {
      throw new KeyException(
          keyError,
          String.format("tikv restart txn %s", keyError.getRetryableBytes().toStringUtf8()));
    }

    if (!keyError.getAbort().isEmpty()) {
      throw new KeyException(
          keyError, String.format("tikv abort txn %s", keyError.getAbortBytes().toStringUtf8()));
    }

    throw new KeyException(
        keyError, String.format("unexpected key error meets and it is %s", keyError.toString()));
  }

  static AbstractLockResolverClient getInstance(
      Metapb.Store store,
      TiConfiguration conf,
      TiRegion region,
      TikvGrpc.TikvBlockingStub blockingStub,
      TikvGrpc.TikvStub asyncStub,
      ChannelFactory channelFactory,
      RegionManager regionManager,
      PDClient pdClient,
      RegionStoreClient.RegionStoreClientBuilder clientBuilder) {
    if (StoreVersion.compareTo(store.getVersion(), Version.RESOLVE_LOCK_V3) < 0) {
      return new LockResolverClientV2(
          conf, region, blockingStub, asyncStub, channelFactory, regionManager);
    } else if (StoreVersion.compareTo(store.getVersion(), Version.RESOLVE_LOCK_V4) < 0) {
      return new LockResolverClientV3(
          conf,
          region,
          blockingStub,
          asyncStub,
          channelFactory,
          regionManager,
          pdClient,
          clientBuilder);
    } else {
      return new LockResolverClientV4(
          conf,
          region,
          blockingStub,
          asyncStub,
          channelFactory,
          regionManager,
          pdClient,
          clientBuilder);
    }
  }

  String getVersion();

  /**
   * ResolveLocks tries to resolve Locks. The resolving process is in 3 steps: 1) Use the `lockTTL`
   * to pick up all expired locks. Only locks that are old enough are considered orphan locks and
   * will be handled later. If all locks are expired then all locks will be resolved so true will be
   * returned, otherwise caller should sleep a while before retry. 2) For each lock, query the
   * primary key to get txn(which left the lock)'s commit status. 3) Send `ResolveLock` cmd to the
   * lock's region to resolve all locks belong to the same transaction.
   *
   * @param bo
   * @param callerStartTS
   * @param locks
   * @param forWrite
   * @return msBeforeTxnExpired: 0 means all locks are resolved
   */
  ResolveLockResult resolveLocks(
      BackOffer bo, long callerStartTS, List<Lock> locks, boolean forWrite);
}
