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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.pingcap.tikv.region.RegionStoreClient.RequestTypes.REQ_TYPE_DAG;
import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.pingcap.tidb.tipb.DAGRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.AbstractGRPCClient;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.exception.*;
import com.pingcap.tikv.operation.KVErrorHandler;
import com.pingcap.tikv.streaming.StreamingResponse;
import com.pingcap.tikv.txn.Lock;
import com.pingcap.tikv.txn.LockResolverClient;
import com.pingcap.tikv.util.*;
import io.grpc.ManagedChannel;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Coprocessor.Request;
import org.tikv.kvproto.Coprocessor.Response;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Kvrpcpb.*;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.TikvGrpc;
import org.tikv.kvproto.TikvGrpc.TikvBlockingStub;
import org.tikv.kvproto.TikvGrpc.TikvStub;

// RegionStore itself is not thread-safe
public class RegionStoreClient extends AbstractGRPCClient<TikvBlockingStub, TikvStub>
    implements RegionErrorReceiver {
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

  private static final Logger logger = Logger.getLogger(RegionStoreClient.class);
  private TiRegion region;
  private final RegionManager regionManager;

  @VisibleForTesting public final LockResolverClient lockResolverClient;
  private TikvBlockingStub blockingStub;
  private TikvStub asyncStub;

  public TiRegion getRegion() {
    return region;
  }

  private boolean checkLockError(BackOffer backOffer, KeyError error) {
    if (error.hasLocked()) {
      Lock lock = new Lock(error.getLocked());
      logger.warn("resolving lock");
      boolean ok =
          lockResolverClient.resolveLocks(
              backOffer, new ArrayList<>(Collections.singletonList(lock)));
      if (!ok) {
        // if not resolve all locks, we wait and retry
        backOffer.doBackOff(BoTxnLockFast, new KeyException((error.getLocked().toString())));
      }
      return false;
    } else {
      // retry or abort
      // this should trigger Spark to retry the txn
      throw new KeyException(error);
    }
  }

  /**
   * Fetch a value according to a key
   *
   * @param backOffer
   * @param key
   * @param version
   * @return
   * @throws TiClientInternalException
   * @throws KeyException
   */
  public ByteString get(BackOffer backOffer, ByteString key, long version)
      throws TiClientInternalException, KeyException {
    while (true) {
      // we should refresh region
      region = regionManager.getRegionByKey(key);

      Supplier<GetRequest> factory =
          () ->
              GetRequest.newBuilder()
                  .setContext(region.getContext())
                  .setKey(key)
                  .setVersion(version)
                  .build();

      KVErrorHandler<GetResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              region,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      GetResponse resp = callWithRetry(backOffer, TikvGrpc.METHOD_KV_GET, factory, handler);

      if (isGetSuccess(backOffer, resp)) {
        return resp.getValue();
      }
    }
  }

  /**
   * @param backOffer
   * @param resp
   * @return Return true means the rpc call success. Return false means the rpc call fail,
   *     RegionStoreClient should retry. Throw an Exception means the rpc call fail,
   *     RegionStoreClient cannot handle this kind of error.
   * @throws TiClientInternalException
   * @throws KeyException
   */
  private boolean isGetSuccess(BackOffer backOffer, GetResponse resp)
      throws TiClientInternalException, KeyException {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("GetResponse failed without a cause");
    }
    if (resp.hasRegionError()) {
      backOffer.doBackOff(BoRegionMiss, new RegionException(resp.getRegionError()));
      return false;
    }

    if (resp.hasError()) {
      return checkLockError(backOffer, resp.getError());
    }
    return true;
  }

  public List<KvPair> batchGet(BackOffer backOffer, List<ByteString> keys, long version) {
    List<KvPair> result = new ArrayList<>();
    while (true) {
      // re-split keys
      Map<TiRegion, List<ByteString>> map =
          keys.stream().collect(Collectors.groupingBy(regionManager::getRegionByKey));
      boolean ok = true;
      for (Map.Entry<TiRegion, List<ByteString>> entry : map.entrySet()) {
        TiRegion newRegion = entry.getKey();
        if (!newRegion.equals(region)) {
          RegionStoreClient newRegionStoreClient =
              new RegionStoreClientBuilder(conf, this.channelFactory, this.regionManager)
                  .build(newRegion);
          result.addAll(newRegionStoreClient.batchGet(backOffer, entry.getValue(), version));
        } else {
          Supplier<BatchGetRequest> request =
              () ->
                  BatchGetRequest.newBuilder()
                      .setContext(region.getContext())
                      .addAllKeys(entry.getValue())
                      .setVersion(version)
                      .build();
          KVErrorHandler<BatchGetResponse> handler =
              new KVErrorHandler<>(
                  regionManager,
                  this,
                  region,
                  resp -> resp.hasRegionError() ? resp.getRegionError() : null);
          BatchGetResponse resp =
              callWithRetry(backOffer, TikvGrpc.METHOD_KV_BATCH_GET, request, handler);
          if (isBatchGetSuccess(backOffer, resp)) {
            result.addAll(resp.getPairsList());
          } else {
            ok = false;
          }
        }
      }
      if (ok) {
        return result;
      }
    }
  }

  private boolean isBatchGetSuccess(BackOffer bo, BatchGetResponse resp) {
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
      boolean ok = lockResolverClient.resolveLocks(bo, locks);
      if (!ok) {
        // if not resolve all locks, we wait and retry
        bo.doBackOff(BoTxnLockFast, new KeyException((resp.getPairsList().get(0).getError())));
        return false;
      }
    }
    return true;
  }

  public List<KvPair> scan(
      BackOffer backOffer, ByteString startKey, long version, boolean keyOnly) {
    while (true) {
      // we should refresh region
      region = regionManager.getRegionByKey(startKey);

      Supplier<ScanRequest> request =
          () ->
              ScanRequest.newBuilder()
                  .setContext(region.getContext())
                  .setStartKey(startKey)
                  .setVersion(version)
                  .setKeyOnly(keyOnly)
                  .setLimit(getConf().getScanBatchSize())
                  .build();

      KVErrorHandler<ScanResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              region,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);
      ScanResponse resp = callWithRetry(backOffer, TikvGrpc.METHOD_KV_SCAN, request, handler);
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
        Lock lock = extractLockFromKeyErr(kvPair.getError());
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

  private Lock extractLockFromKeyErr(KeyError keyError) {
    if (keyError.hasLocked()) {
      return new Lock(keyError.getLocked());
    }

    if (keyError.hasConflict()) {
      WriteConflict conflict = keyError.getConflict();
      throw new KeyException(
          String.format(
              "scan meet key conflict on primary key %s at commit ts %s",
              conflict.getPrimary(), conflict.getConflictTs()));
    }

    if (!keyError.getRetryable().isEmpty()) {
      throw new KeyException(
          String.format("tikv restart txn %s", keyError.getRetryableBytes().toStringUtf8()));
    }

    if (!keyError.getAbort().isEmpty()) {
      throw new KeyException(
          String.format("tikv abort txn %s", keyError.getAbortBytes().toStringUtf8()));
    }

    throw new KeyException(
        String.format("unexpected key error meets and it is %s", keyError.toString()));
  }

  public List<KvPair> scan(BackOffer backOffer, ByteString startKey, long version) {
    return scan(backOffer, startKey, version, false);
  }

  /**
   * Prewrite batch keys
   *
   * @param backOffer
   * @param primary
   * @param mutations
   * @param startTs
   * @param lockTTL
   * @throws TiClientInternalException
   * @throws KeyException
   * @throws RegionException
   */
  public void prewrite(
      BackOffer backOffer,
      ByteString primary,
      Iterable<Mutation> mutations,
      long startTs,
      long lockTTL)
      throws TiClientInternalException, KeyException, RegionException {
    this.prewrite(backOffer, primary, mutations, startTs, lockTTL, false);
  }

  /**
   * Prewrite batch keys
   *
   * @param bo
   * @param primaryLock
   * @param mutations
   * @param startVersion
   * @param ttl
   * @param skipConstraintCheck
   * @throws TiClientInternalException
   * @throws KeyException
   * @throws RegionException
   */
  public void prewrite(
      BackOffer bo,
      ByteString primaryLock,
      Iterable<Mutation> mutations,
      long startVersion,
      long ttl,
      boolean skipConstraintCheck)
      throws TiClientInternalException, KeyException, RegionException {
    while (true) {
      Supplier<PrewriteRequest> factory =
          () ->
              PrewriteRequest.newBuilder()
                  .setContext(region.getContext())
                  .setStartVersion(startVersion)
                  .setPrimaryLock(primaryLock)
                  .addAllMutations(mutations)
                  .setLockTtl(ttl)
                  .setSkipConstraintCheck(skipConstraintCheck)
                  .build();
      KVErrorHandler<PrewriteResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              region,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);
      PrewriteResponse resp = callWithRetry(bo, TikvGrpc.METHOD_KV_PREWRITE, factory, handler);
      if (isPrewriteSuccess(bo, resp)) {
        return;
      }
    }
  }

  /**
   * @param backOffer
   * @param resp
   * @return Return true means the rpc call success. Return false means the rpc call fail,
   *     RegionStoreClient should retry. Throw an Exception means the rpc call fail,
   *     RegionStoreClient cannot handle this kind of error
   * @throws TiClientInternalException
   * @throws RegionException
   * @throws KeyException
   */
  private boolean isPrewriteSuccess(BackOffer backOffer, PrewriteResponse resp)
      throws TiClientInternalException, KeyException, RegionException {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("Prewrite Response failed without a cause");
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }

    boolean result = true;
    List<Lock> locks = new ArrayList<>();
    for (KeyError err : resp.getErrorsList()) {
      if (err.hasLocked()) {
        result = false;
        Lock lock = new Lock(err.getLocked());
        locks.add(lock);
      } else {
        throw new KeyException(err.toString());
      }
    }

    if (!lockResolverClient.resolveLocks(backOffer, locks)) {
      backOffer.doBackOff(BoTxnLock, new KeyException(resp.getErrorsList().get(0)));
    }
    return result;
  }

  /**
   * Commit batch keys
   *
   * @param backOffer
   * @param keys
   * @param startVersion
   * @param commitVersion
   */
  public void commit(
      BackOffer backOffer, Iterable<ByteString> keys, long startVersion, long commitVersion)
      throws KeyException {
    while (true) {
      Supplier<CommitRequest> factory =
          () ->
              CommitRequest.newBuilder()
                  .setStartVersion(startVersion)
                  .setCommitVersion(commitVersion)
                  .addAllKeys(keys)
                  .setContext(region.getContext())
                  .build();
      KVErrorHandler<CommitResponse> handler =
          new KVErrorHandler<>(
              regionManager,
              this,
              region,
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);
      CommitResponse resp = callWithRetry(backOffer, TikvGrpc.METHOD_KV_COMMIT, factory, handler);
      if (isCommitSuccess(backOffer, resp)) {
        break;
      }
    }
  }

  /**
   * @param backOffer
   * @param resp
   * @return Return true means the rpc call success. Return false means the rpc call fail,
   *     RegionStoreClient should retry. Throw an Exception means the rpc call fail,
   *     RegionStoreClient cannot handle this kind of error
   * @throws TiClientInternalException
   * @throws RegionException
   * @throws KeyException
   */
  private boolean isCommitSuccess(BackOffer backOffer, CommitResponse resp)
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
      return checkLockError(backOffer, resp.getError());
    }
    return true;
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
      Queue<SelectResponse> responseQueue) {
    if (req == null || ranges == null || req.getExecutorsCount() < 1) {
      throw new IllegalArgumentException("Invalid coprocess argument!");
    }

    Supplier<Coprocessor.Request> reqToSend =
        () ->
            Coprocessor.Request.newBuilder()
                .setContext(region.getContext())
                .setTp(REQ_TYPE_DAG.getValue())
                .setData(req.toByteString())
                .addAllRanges(ranges)
                .build();

    // we should handle the region error ourselves
    KVErrorHandler<Coprocessor.Response> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    Coprocessor.Response resp =
        callWithRetry(backOffer, TikvGrpc.METHOD_COPROCESSOR, reqToSend, handler);
    return handleCopResponse(backOffer, resp, ranges, responseQueue);
  }

  // handleCopResponse checks coprocessor Response for region split and lock,
  // returns more tasks when that happens, or handles the response if no error.
  // if we're handling streaming coprocessor response, lastRange is the range of last
  // successful response, otherwise it's nil.
  private List<RangeSplitter.RegionTask> handleCopResponse(
      BackOffer backOffer,
      Coprocessor.Response response,
      List<KeyRange> ranges,
      Queue<SelectResponse> responseQueue) {
    if (response.hasRegionError()) {
      Errorpb.Error regionError = response.getRegionError();
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(regionError.toString()));
      logger.warn("Re-splitting region task due to region error:" + regionError.getMessage());
      // Split ranges
      return RangeSplitter.newSplitter(this.regionManager).splitRangeByRegion(ranges);
    }

    if (response.hasLocked()) {
      Lock lock = new Lock(response.getLocked());
      logger.debug(String.format("coprocessor encounters locks: %s", lock));
      boolean ok =
          lockResolverClient.resolveLocks(
              backOffer, new ArrayList<>(Collections.singletonList(lock)));
      if (!ok) {
        backOffer.doBackOff(BoTxnLockFast, new LockException(lock));
      }
      // Split ranges
      return RangeSplitter.newSplitter(this.regionManager).splitRangeByRegion(ranges);
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
  public Iterator<SelectResponse> coprocessStreaming(DAGRequest req, List<KeyRange> ranges) {
    Supplier<Request> reqToSend =
        () ->
            Coprocessor.Request.newBuilder()
                .setContext(region.getContext())
                // TODO: If no executors...?
                .setTp(REQ_TYPE_DAG.getValue())
                .setData(req.toByteString())
                .addAllRanges(ranges)
                .build();

    KVErrorHandler<StreamingResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            region,
            StreamingResponse::getFirstRegionError // TODO: handle all errors in streaming response
            );

    StreamingResponse responseIterator =
        this.callServerStreamingWithRetry(
            ConcreteBackOffer.newCopNextMaxBackOff(),
            TikvGrpc.METHOD_COPROCESSOR_STREAM,
            reqToSend,
            handler);
    return doCoprocessor(responseIterator);
  }

  public static class RegionStoreClientBuilder {
    private final TiConfiguration conf;
    private final ChannelFactory channelFactory;
    private final RegionManager regionManager;

    public RegionStoreClientBuilder(
        TiConfiguration conf, ChannelFactory channelFactory, RegionManager regionManager) {
      Objects.requireNonNull(conf, "conf is null");
      Objects.requireNonNull(channelFactory, "channelFactory is null");
      Objects.requireNonNull(regionManager, "regionManager is null");
      this.conf = conf;
      this.channelFactory = channelFactory;
      this.regionManager = regionManager;
    }

    public RegionStoreClient build(TiRegion region, Store store) {
      Objects.requireNonNull(region, "region is null");
      Objects.requireNonNull(store, "store is null");

      String addressStr = store.getAddress();
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("Create region store client on address %s", addressStr));
      }
      ManagedChannel channel = channelFactory.getChannel(addressStr);

      TikvBlockingStub blockingStub = TikvGrpc.newBlockingStub(channel);
      TikvStub asyncStub = TikvGrpc.newStub(channel);

      return new RegionStoreClient(
          conf, region, channelFactory, blockingStub, asyncStub, regionManager);
    }

    public RegionStoreClient build(ByteString key) {
      Pair<TiRegion, Store> pair = regionManager.getRegionStorePairByKey(key);
      return build(pair.first, pair.second);
    }

    public RegionStoreClient build(TiRegion region) {
      Store store = regionManager.getStoreById(region.getLeader().getStoreId());
      return build(region, store);
    }

    public RegionManager getRegionManager() {
      return regionManager;
    }
  }

  private RegionStoreClient(
      TiConfiguration conf,
      TiRegion region,
      ChannelFactory channelFactory,
      TikvBlockingStub blockingStub,
      TikvStub asyncStub,
      RegionManager regionManager) {
    super(conf, channelFactory);
    checkNotNull(region, "Region is empty");
    checkNotNull(region.getLeader(), "Leader Peer is null");
    checkArgument(region.getLeader() != null, "Leader Peer is null");
    this.regionManager = regionManager;
    this.region = region;
    this.blockingStub = blockingStub;
    this.asyncStub = asyncStub;
    this.lockResolverClient =
        new LockResolverClient(
            conf, this.blockingStub, this.asyncStub, channelFactory, regionManager);
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
    String addressStr = regionManager.getStoreById(region.getLeader().getStoreId()).getAddress();
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
}
