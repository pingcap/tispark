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

package com.pingcap.tikv.region;

import static com.pingcap.tikv.region.RegionStoreClient.RequestTypes.REQ_TYPE_DAG;
import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoRegionMiss;
import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoTxnLock;
import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoTxnLockFast;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.pingcap.tidb.tipb.DAGRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.PDClient;
import com.pingcap.tikv.StoreVersion;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.Version;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.exception.LockException;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.exception.SelectException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.exception.TiKVException;
import com.pingcap.tikv.operation.KVErrorHandler;
import com.pingcap.tikv.streaming.StreamingResponse;
import com.pingcap.tikv.txn.AbstractLockResolverClient;
import com.pingcap.tikv.txn.Lock;
import com.pingcap.tikv.txn.ResolveLockResult;
import com.pingcap.tikv.util.BackOffFunction;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ChannelFactory;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;
import com.pingcap.tikv.util.RangeSplitter;
import io.grpc.ManagedChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Coprocessor.Request;
import org.tikv.kvproto.Coprocessor.Response;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Kvrpcpb.BatchGetRequest;
import org.tikv.kvproto.Kvrpcpb.BatchGetResponse;
import org.tikv.kvproto.Kvrpcpb.CommitRequest;
import org.tikv.kvproto.Kvrpcpb.CommitResponse;
import org.tikv.kvproto.Kvrpcpb.GetRequest;
import org.tikv.kvproto.Kvrpcpb.GetResponse;
import org.tikv.kvproto.Kvrpcpb.KeyError;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.kvproto.Kvrpcpb.Mutation;
import org.tikv.kvproto.Kvrpcpb.PrewriteRequest;
import org.tikv.kvproto.Kvrpcpb.PrewriteResponse;
import org.tikv.kvproto.Kvrpcpb.ScanRequest;
import org.tikv.kvproto.Kvrpcpb.ScanResponse;
import org.tikv.kvproto.Kvrpcpb.SplitRegionRequest;
import org.tikv.kvproto.Kvrpcpb.SplitRegionResponse;
import org.tikv.kvproto.Kvrpcpb.TxnHeartBeatRequest;
import org.tikv.kvproto.Kvrpcpb.TxnHeartBeatResponse;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.TikvGrpc;
import org.tikv.kvproto.TikvGrpc.TikvBlockingStub;
import org.tikv.kvproto.TikvGrpc.TikvStub;

// TODO:
//  1. RegionStoreClient will be inaccessible directly.
//  2. All apis of RegionStoreClient would not provide retry aside from callWithRetry,
//  if a request needs to be retried because of an un-retryable cause, e.g., keys
//  need to be re-split across regions/stores, region info outdated, e.t.c., you
//  should retry it in an upper client logic (KVClient, TxnClient, e.t.c.)

/** Note that RegionStoreClient itself is not thread-safe */
public class RegionStoreClient extends AbstractRegionStoreClient {
  private static final Logger logger = LoggerFactory.getLogger(RegionStoreClient.class);
  @VisibleForTesting public final AbstractLockResolverClient lockResolverClient;
  private final TiStoreType storeType;
  /** startTS -> List(locks) */
  private final Map<Long, Set<Long>> resolvedLocks = new HashMap<>();

  private final PDClient pdClient;
  private Boolean isV4 = null;

  private synchronized Boolean getIsV4() {
    if (isV4 == null) {
      isV4 = StoreVersion.minTiKVVersion(Version.RESOLVE_LOCK_V4, pdClient);
    }
    return isV4;
  }

  private RegionStoreClient(
      TiConfiguration conf,
      TiRegion region,
      Store store,
      TiStoreType storeType,
      ChannelFactory channelFactory,
      TikvBlockingStub blockingStub,
      TikvStub asyncStub,
      RegionManager regionManager,
      PDClient pdClient,
      RegionStoreClient.RegionStoreClientBuilder clientBuilder) {
    super(conf, region, channelFactory, blockingStub, asyncStub, regionManager);
    this.storeType = storeType;

    if (this.storeType == TiStoreType.TiKV) {
      this.lockResolverClient =
          AbstractLockResolverClient.getInstance(
              store,
              conf,
              region,
              this.blockingStub,
              this.asyncStub,
              channelFactory,
              regionManager,
              pdClient,
              clientBuilder);

    } else {
      Store tikvStore =
          regionManager.getRegionStorePairByKey(region.getStartKey(), TiStoreType.TiKV).second;

      String addressStr = tikvStore.getAddress();
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("Create region store client on address %s", addressStr));
      }
      ManagedChannel channel = channelFactory.getChannel(addressStr);

      TikvBlockingStub tikvBlockingStub = TikvGrpc.newBlockingStub(channel);
      TikvStub tikvAsyncStub = TikvGrpc.newStub(channel);

      this.lockResolverClient =
          AbstractLockResolverClient.getInstance(
              tikvStore,
              conf,
              region,
              tikvBlockingStub,
              tikvAsyncStub,
              channelFactory,
              regionManager,
              pdClient,
              clientBuilder);
    }
    this.pdClient = pdClient;
  }

  public synchronized boolean addResolvedLocks(Long version, Set<Long> locks) {
    Set<Long> oldList = resolvedLocks.get(version);
    if (oldList != null) {
      oldList.addAll(locks);
    } else {
      resolvedLocks.put(version, new HashSet<>(locks));
    }
    return true;
  }

  public synchronized Set<Long> getResolvedLocks(Long version) {
    return resolvedLocks.getOrDefault(version, java.util.Collections.emptySet());
  }

  /**
   * Fetch a value according to a key
   *
   * @param backOffer backOffer
   * @param key key to fetch
   * @param version key version
   * @return value
   * @throws TiClientInternalException TiSpark Client exception, unexpected
   * @throws KeyException Key may be locked
   */
  public ByteString get(BackOffer backOffer, ByteString key, long version)
      throws TiClientInternalException, KeyException {
    boolean forWrite = false;
    Supplier<GetRequest> factory =
        () ->
            GetRequest.newBuilder()
                .setContext(region.getContext(getResolvedLocks(version)))
                .setKey(key)
                .setVersion(version)
                .build();

    KVErrorHandler<GetResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            lockResolverClient,
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null,
            resp -> resp.hasError() ? resp.getError() : null,
            resolveLockResult -> addResolvedLocks(version, resolveLockResult.getResolvedLocks()),
            version,
            forWrite);

    GetResponse resp = callWithRetry(backOffer, TikvGrpc.getKvGetMethod(), factory, handler);

    handleGetResponse(resp);
    return resp.getValue();
  }

  /**
   * @param resp GetResponse
   * @throws TiClientInternalException TiSpark Client exception, unexpected
   * @throws KeyException Key may be locked
   */
  private void handleGetResponse(GetResponse resp) throws TiClientInternalException, KeyException {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("GetResponse failed without a cause");
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    if (resp.hasError()) {
      throw new KeyException(resp.getError());
    }
  }

  public List<KvPair> batchGet(BackOffer backOffer, List<ByteString> keys, long version) {
    boolean forWrite = false;
    Supplier<BatchGetRequest> request =
        () ->
            BatchGetRequest.newBuilder()
                .setContext(region.getContext(getResolvedLocks(version)))
                .addAllKeys(keys)
                .setVersion(version)
                .build();
    KVErrorHandler<BatchGetResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            lockResolverClient,
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null,
            resp -> null,
            resolveLockResult -> addResolvedLocks(version, resolveLockResult.getResolvedLocks()),
            version,
            forWrite);
    BatchGetResponse resp =
        callWithRetry(backOffer, TikvGrpc.getKvBatchGetMethod(), request, handler);

    try {
      return handleBatchGetResponse(backOffer, resp, version);
    } catch (TiKVException e) {
      if ("locks not resolved, retry".equals(e.getMessage())) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoTxnLock, e);
        return batchGet(backOffer, keys, version);
      } else {
        throw e;
      }
    }
  }

  private List<KvPair> handleBatchGetResponse(
      BackOffer backOffer, BatchGetResponse resp, long version) {
    boolean forWrite = false;
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("BatchGetResponse failed without a cause");
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    List<Lock> locks = new ArrayList<>();

    for (KvPair pair : resp.getPairsList()) {
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
      ResolveLockResult resolveLockResult =
          lockResolverClient.resolveLocks(backOffer, version, locks, forWrite);
      addResolvedLocks(version, resolveLockResult.getResolvedLocks());
      // resolveLocks already retried, just throw error to upper logic.
      throw new TiKVException("locks not resolved, retry");
    } else {
      return resp.getPairsList();
    }
  }

  public List<KvPair> scan(
      BackOffer backOffer, ByteString startKey, long version, boolean keyOnly) {
    boolean forWrite = false;
    while (true) {
      // we should refresh region
      region = regionManager.getRegionByKey(startKey);

      Supplier<ScanRequest> request =
          () ->
              ScanRequest.newBuilder()
                  .setContext(region.getContext(getResolvedLocks(version)))
                  .setStartKey(startKey)
                  .setVersion(version)
                  .setKeyOnly(keyOnly)
                  .setLimit(getConf().getScanBatchSize())
                  .build();

      KVErrorHandler<ScanResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              lockResolverClient,
              region,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null,
              resp -> null,
              resolveLockResult -> addResolvedLocks(version, resolveLockResult.getResolvedLocks()),
              version,
              forWrite);
      ScanResponse resp = callWithRetry(backOffer, TikvGrpc.getKvScanMethod(), request, handler);
      if (isScanSuccess(backOffer, resp)) {
        return doScan(resp);
      }
    }
  }

  private boolean isScanSuccess(BackOffer backOffer, ScanResponse resp) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("ScanResponse failed without a cause");
    }
    if (resp.hasRegionError()) {
      backOffer.doBackOff(BoRegionMiss, new RegionException(resp.getRegionError()));
      return false;
    }
    return true;
  }

  // TODO: resolve locks after scan
  private List<KvPair> doScan(ScanResponse resp) {
    // Check if kvPair contains error, it should be a Lock if hasError is true.
    List<KvPair> kvPairs = resp.getPairsList();
    List<KvPair> newKvPairs = new ArrayList<>();
    for (KvPair kvPair : kvPairs) {
      if (kvPair.hasError()) {
        Lock lock = AbstractLockResolverClient.extractLockFromKeyErr(kvPair.getError());
        newKvPairs.add(
            KvPair.newBuilder()
                .setError(kvPair.getError())
                .setValue(kvPair.getValue())
                .setKey(lock.getKey())
                .build());
      } else {
        newKvPairs.add(kvPair);
      }
    }
    return Collections.unmodifiableList(newKvPairs);
  }

  public List<KvPair> scan(BackOffer backOffer, ByteString startKey, long version) {
    return scan(backOffer, startKey, version, false);
  }

  /**
   * Prewrite batch keys
   *
   * @param backOffer backOffer
   * @param primary primary lock of keys
   * @param mutations batch key-values as mutations
   * @param startTs startTs of prewrite
   * @param lockTTL lock ttl
   * @throws TiClientInternalException TiSpark Client exception, unexpected
   * @throws KeyException Key may be locked
   * @throws RegionException region error occurs
   */
  public void prewrite(
      BackOffer backOffer,
      ByteString primary,
      Iterable<Mutation> mutations,
      long startTs,
      long lockTTL)
      throws TiClientInternalException, KeyException, RegionException {
    this.prewrite(backOffer, primary, mutations, startTs, lockTTL, false, false, null);
  }

  /**
   * Prewrite batch keys
   *
   * @param skipConstraintCheck whether to skip constraint check
   */
  public void prewrite(
      BackOffer bo,
      ByteString primaryLock,
      Iterable<Mutation> mutations,
      long startTs,
      long ttl,
      boolean skipConstraintCheck,
      boolean useAsyncCommit,
      Iterable<ByteString> secondaries)
      throws TiClientInternalException, KeyException, RegionException {
    boolean forWrite = true;
    while (true) {
      Supplier<PrewriteRequest> factory =
          () -> {
            PrewriteRequest.Builder builder =
                PrewriteRequest.newBuilder()
                    .setContext(region.getContext())
                    .setStartVersion(startTs)
                    .setPrimaryLock(primaryLock)
                    .addAllMutations(mutations)
                    .setLockTtl(ttl)
                    .setSkipConstraintCheck(skipConstraintCheck)
                    .setTxnSize(16);

            if (getIsV4()) {
              builder.setMinCommitTs(startTs);
            }

            if (useAsyncCommit) {
              builder.setUseAsyncCommit(true);

              if (secondaries != null) {
                builder.addAllSecondaries(secondaries);
              }
            }
            return builder.build();
          };
      KVErrorHandler<PrewriteResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              lockResolverClient,
              region,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null,
              resp -> null,
              resolveLockResult -> null,
              startTs,
              forWrite);
      PrewriteResponse resp = callWithRetry(bo, TikvGrpc.getKvPrewriteMethod(), factory, handler);
      if (isPrewriteSuccess(bo, resp, startTs)) {
        return;
      }
    }
  }

  /**
   * @param backOffer backOffer
   * @param resp response
   * @return Return true means the rpc call success. Return false means the rpc call fail,
   *     RegionStoreClient should retry. Throw an Exception means the rpc call fail,
   *     RegionStoreClient cannot handle this kind of error
   * @throws TiClientInternalException
   * @throws RegionException
   * @throws KeyException
   */
  private boolean isPrewriteSuccess(BackOffer backOffer, PrewriteResponse resp, long startTs)
      throws TiClientInternalException, KeyException, RegionException {
    boolean forWrite = true;
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("Prewrite Response failed without a cause");
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }

    boolean isSuccess = true;
    List<Lock> locks = new ArrayList<>();
    for (KeyError err : resp.getErrorsList()) {
      if (err.hasLocked()) {
        isSuccess = false;
        Lock lock = new Lock(err.getLocked());
        locks.add(lock);
      } else {
        throw new KeyException(err, err.toString());
      }
    }
    if (isSuccess) {
      return true;
    }

    ResolveLockResult resolveLockResult =
        lockResolverClient.resolveLocks(backOffer, startTs, locks, forWrite);
    addResolvedLocks(startTs, resolveLockResult.getResolvedLocks());
    long msBeforeExpired = resolveLockResult.getMsBeforeTxnExpired();
    if (msBeforeExpired > 0) {
      backOffer.doBackOffWithMaxSleep(
          BoTxnLock, msBeforeExpired, new KeyException(resp.getErrorsList().get(0)));
    }
    return false;
  }

  /** TXN Heart Beat: update primary key ttl */
  public void txnHeartBeat(BackOffer bo, ByteString primaryLock, long startTs, long ttl) {
    boolean forWrite = false;
    while (true) {
      Supplier<TxnHeartBeatRequest> factory =
          () ->
              TxnHeartBeatRequest.newBuilder()
                  .setContext(region.getContext())
                  .setStartVersion(startTs)
                  .setPrimaryLock(primaryLock)
                  .setAdviseLockTtl(ttl)
                  .build();
      KVErrorHandler<TxnHeartBeatResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              lockResolverClient,
              region,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null,
              resp -> resp.hasError() ? resp.getError() : null,
              resolveLockResult -> null,
              startTs,
              forWrite);
      TxnHeartBeatResponse resp =
          callWithRetry(bo, TikvGrpc.getKvTxnHeartBeatMethod(), factory, handler);
      if (isTxnHeartBeatSuccess(resp)) {
        return;
      }
    }
  }

  private boolean isTxnHeartBeatSuccess(TxnHeartBeatResponse resp)
      throws TiClientInternalException, RegionException {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("TxnHeartBeat Response failed without a cause");
    }

    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }

    if (resp.hasError()) {
      throw new TiClientInternalException("TxnHeartBeat fail, " + resp.getError().getAbort());
    }

    return true;
  }

  /**
   * Commit batch keys
   *
   * @param backOffer backOffer
   * @param keys keys to commit
   * @param startTs start version
   * @param commitTs commit version
   */
  public void commit(BackOffer backOffer, Iterable<ByteString> keys, long startTs, long commitTs)
      throws KeyException {
    boolean forWrite = true;
    Supplier<CommitRequest> factory =
        () ->
            CommitRequest.newBuilder()
                .setStartVersion(startTs)
                .setCommitVersion(commitTs)
                .addAllKeys(keys)
                .setContext(region.getContext())
                .build();
    KVErrorHandler<CommitResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            lockResolverClient,
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null,
            resp -> resp.hasError() ? resp.getError() : null,
            resolveLockResult -> null,
            startTs,
            forWrite);
    CommitResponse resp = callWithRetry(backOffer, TikvGrpc.getKvCommitMethod(), factory, handler);
    handleCommitResponse(resp);
  }

  /**
   * @param resp CommitResponse
   * @throws TiClientInternalException
   * @throws RegionException
   * @throws KeyException
   */
  private void handleCommitResponse(CommitResponse resp)
      throws TiClientInternalException, RegionException, KeyException {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("CommitResponse failed without a cause");
    }
    if (resp.hasRegionError()) {
      // bo.doBackOff(BoRegionMiss, new RegionException(resp.getRegionError()));
      // return false;
      // Caller method should restart commit
      throw new RegionException(resp.getRegionError());
    }
    // If we find locks, we first resolve and let its caller retry.
    if (resp.hasError()) {
      throw new KeyException(resp.getError());
    }
  }

  /**
   * Execute and retrieve the response from TiKV server.
   *
   * @param req Select request to process
   * @param ranges Key range list
   * @return Remaining tasks of this request, if task split happens, null otherwise
   */
  public List<RangeSplitter.RegionTask> coprocess(
      BackOffer backOffer,
      DAGRequest req,
      List<KeyRange> ranges,
      Queue<SelectResponse> responseQueue,
      long startTs) {
    boolean forWrite = false;
    if (req == null || ranges == null || req.getExecutorsCount() < 1) {
      throw new IllegalArgumentException("Invalid coprocessor argument!");
    }

    Supplier<Coprocessor.Request> reqToSend =
        () ->
            Coprocessor.Request.newBuilder()
                .setContext(region.getContext(getResolvedLocks(startTs)))
                .setTp(REQ_TYPE_DAG.getValue())
                .setStartTs(startTs)
                .setData(req.toByteString())
                .addAllRanges(ranges)
                .build();

    // we should handle the region error ourselves
    KVErrorHandler<Coprocessor.Response> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            lockResolverClient,
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null,
            resp -> null,
            resolveLockResult -> addResolvedLocks(startTs, resolveLockResult.getResolvedLocks()),
            startTs,
            forWrite);
    Coprocessor.Response resp =
        callWithRetry(backOffer, TikvGrpc.getCoprocessorMethod(), reqToSend, handler);
    return handleCopResponse(backOffer, resp, ranges, responseQueue, startTs);
  }

  // handleCopResponse checks coprocessor Response for region split and lock,
  // returns more tasks when that happens, or handles the response if no error.
  // if we're handling streaming coprocessor response, lastRange is the range of last
  // successful response, otherwise it's nil.
  private List<RangeSplitter.RegionTask> handleCopResponse(
      BackOffer backOffer,
      Coprocessor.Response response,
      List<KeyRange> ranges,
      Queue<SelectResponse> responseQueue,
      long startTs) {
    boolean forWrite = false;
    if (response == null) {
      // Send request failed, reasons may:
      // 1. TiKV down
      // 2. Network partition
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoRegionMiss,
          new GrpcException("TiKV down or Network partition"));
      logger.warn("Re-splitting region task due to region error: TiKV down or Network partition");
      // Split ranges
      return RangeSplitter.newSplitter(this.regionManager).splitRangeByRegion(ranges, storeType);
    }

    if (response.hasRegionError()) {
      Errorpb.Error regionError = response.getRegionError();
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(regionError.toString()));
      logger.warn("Re-splitting region task due to region error:" + regionError.getMessage());
      // Split ranges
      return RangeSplitter.newSplitter(this.regionManager).splitRangeByRegion(ranges, storeType);
    }

    if (response.hasLocked()) {
      Lock lock = new Lock(response.getLocked());
      logger.debug(String.format("coprocessor encounters locks: %s", lock));
      ResolveLockResult resolveLockResult =
          lockResolverClient.resolveLocks(
              backOffer, startTs, Collections.singletonList(lock), forWrite);
      addResolvedLocks(startTs, resolveLockResult.getResolvedLocks());
      long msBeforeExpired = resolveLockResult.getMsBeforeTxnExpired();
      if (msBeforeExpired > 0) {
        backOffer.doBackOffWithMaxSleep(BoTxnLockFast, msBeforeExpired, new LockException(lock));
      }
      // Split ranges
      return RangeSplitter.newSplitter(this.regionManager).splitRangeByRegion(ranges, storeType);
    }

    String otherError = response.getOtherError();
    if (otherError != null && !otherError.isEmpty()) {
      logger.warn(String.format("Other error occurred, message: %s", otherError));
      throw new GrpcException(otherError);
    }

    responseQueue.offer(doCoprocessor(response));
    return null;
  }

  private Iterator<SelectResponse> doCoprocessor(StreamingResponse response) {
    Iterator<Response> responseIterator = response.iterator();
    // If we got nothing to handle, return null
    if (!responseIterator.hasNext()) {
      return null;
    }

    // Simply wrap it
    return new Iterator<SelectResponse>() {
      @Override
      public boolean hasNext() {
        return responseIterator.hasNext();
      }

      @Override
      public SelectResponse next() {
        return doCoprocessor(responseIterator.next());
      }
    };
  }

  private SelectResponse doCoprocessor(Response resp) {
    try {
      SelectResponse selectResp = SelectResponse.parseFrom(resp.getData());
      if (selectResp.hasError()) {
        throw new SelectException(selectResp.getError(), selectResp.getError().getMsg());
      }
      return selectResp;
    } catch (InvalidProtocolBufferException e) {
      throw new TiClientInternalException("Error parsing protobuf for coprocessor response.", e);
    }
  }

  // TODO: wait for future fix
  // coprocessStreaming doesn't handle split error
  // future work should handle it and do the resolve
  // locks correspondingly
  public Iterator<SelectResponse> coprocessStreaming(
      DAGRequest req, List<KeyRange> ranges, long startTs) {
    boolean forWrite = false;
    Supplier<Request> reqToSend =
        () ->
            Coprocessor.Request.newBuilder()
                .setContext(region.getContext(getResolvedLocks(startTs)))
                // TODO: If no executors...?
                .setTp(REQ_TYPE_DAG.getValue())
                .setData(req.toByteString())
                .addAllRanges(ranges)
                .build();

    KVErrorHandler<StreamingResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            lockResolverClient,
            region,
            StreamingResponse::getFirstRegionError, // TODO: handle all errors in streaming response
            resp -> null,
            resolveLockResult -> addResolvedLocks(startTs, resolveLockResult.getResolvedLocks()),
            startTs,
            forWrite);

    StreamingResponse responseIterator =
        this.callServerStreamingWithRetry(
            ConcreteBackOffer.newCopNextMaxBackOff(),
            TikvGrpc.getCoprocessorStreamMethod(),
            reqToSend,
            handler);
    return doCoprocessor(responseIterator);
  }

  /**
   * Send SplitRegion request to tikv split a region at splitKey. splitKey must between current
   * region's start key and end key.
   *
   * @param splitKeys is the split points for a specific region.
   * @return a split region info.
   */
  public List<TiRegion> splitRegion(Iterable<ByteString> splitKeys) {
    Supplier<SplitRegionRequest> request =
        () ->
            SplitRegionRequest.newBuilder()
                .setContext(region.getContext())
                .addAllSplitKeys(splitKeys)
                .build();

    KVErrorHandler<SplitRegionResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            null,
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null,
            resp -> null,
            resolveLockResult -> null,
            0L,
            false);

    SplitRegionResponse resp =
        callWithRetry(
            ConcreteBackOffer.newGetBackOff(), TikvGrpc.getSplitRegionMethod(), request, handler);

    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("SplitRegion Response failed without a cause");
    }

    if (resp.hasRegionError()) {
      throw new TiClientInternalException(
          String.format(
              "failed to split region %d because %s",
              region.getId(), resp.getRegionError().toString()));
    }

    return resp.getRegionsList()
        .stream()
        .map(
            region ->
                new TiRegion(region, null, conf.getIsolationLevel(), conf.getCommandPriority()))
        .collect(Collectors.toList());
  }

  public enum RequestTypes {
    REQ_TYPE_SELECT(101),
    REQ_TYPE_INDEX(102),
    REQ_TYPE_DAG(103),
    REQ_TYPE_ANALYZE(104),
    BATCH_ROW_COUNT(64);

    private final int value;

    RequestTypes(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  public static class RegionStoreClientBuilder {
    private final TiConfiguration conf;
    private final ChannelFactory channelFactory;
    private final RegionManager regionManager;
    private final PDClient pdClient;

    public RegionStoreClientBuilder(
        TiConfiguration conf,
        ChannelFactory channelFactory,
        RegionManager regionManager,
        PDClient pdClient) {
      Objects.requireNonNull(conf, "conf is null");
      Objects.requireNonNull(channelFactory, "channelFactory is null");
      Objects.requireNonNull(regionManager, "regionManager is null");
      this.conf = conf;
      this.channelFactory = channelFactory;
      this.regionManager = regionManager;
      this.pdClient = pdClient;
    }

    public RegionStoreClient build(TiRegion region, Store store, TiStoreType storeType)
        throws GrpcException {
      Objects.requireNonNull(region, "region is null");
      Objects.requireNonNull(store, "store is null");
      Objects.requireNonNull(storeType, "storeType is null");

      String addressStr = store.getAddress();
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("Create region store client on address %s", addressStr));
      }
      ManagedChannel channel = channelFactory.getChannel(addressStr);

      TikvBlockingStub blockingStub = TikvGrpc.newBlockingStub(channel);
      TikvStub asyncStub = TikvGrpc.newStub(channel);

      return new RegionStoreClient(
          conf,
          region,
          store,
          storeType,
          channelFactory,
          blockingStub,
          asyncStub,
          regionManager,
          pdClient,
          this);
    }

    public RegionStoreClient build(TiRegion region, Store store) throws GrpcException {
      return build(region, store, TiStoreType.TiKV);
    }

    public RegionStoreClient build(ByteString key) throws GrpcException {
      return build(key, TiStoreType.TiKV);
    }

    public RegionStoreClient build(ByteString key, TiStoreType storeType) throws GrpcException {
      Pair<TiRegion, Store> pair = regionManager.getRegionStorePairByKey(key, storeType);
      return build(pair.first, pair.second, storeType);
    }

    public RegionStoreClient build(TiRegion region) throws GrpcException {
      Store store = regionManager.getStoreById(region.getLeader().getStoreId());
      return build(region, store, TiStoreType.TiKV);
    }

    public RegionManager getRegionManager() {
      return regionManager;
    }
  }
}
