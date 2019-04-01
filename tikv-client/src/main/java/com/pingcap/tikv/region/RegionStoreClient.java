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
import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoRegionMiss;
import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoTxnLock;
import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoTxnLockFast;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.pingcap.tidb.tipb.DAGRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.AbstractGRPCClient;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.exception.LockException;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.exception.SelectException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.operation.KVErrorHandler;
import com.pingcap.tikv.streaming.StreamingResponse;
import com.pingcap.tikv.txn.Lock;
import com.pingcap.tikv.txn.LockResolverClient;
import com.pingcap.tikv.util.BackOffFunction;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ChannelFactory;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;
import com.pingcap.tikv.util.RangeSplitter;
import io.grpc.ManagedChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.function.Supplier;
import org.apache.log4j.Logger;
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
  private final TiSession session;

  @VisibleForTesting public final LockResolverClient lockResolverClient;
  private TikvBlockingStub blockingStub;
  private TikvStub asyncStub;

  public TiRegion getRegion() {
    return region;
  }

  /**
   * Fetch a value according to a key
   *
   * @param backOffer means a backoffer polich
   * @param key key-value pair's key which can be used to get a value
   * @param version specifies a particular mvcc version.
   * @return a value associated with key at particular version.
   * @throws TiClientInternalException when resp is null
   * @throws KeyException when fails to resolve lock.
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
   * Check Get successes or not
   * @param backOffer specifies backooffer policy
   * @param resp specifies a GetResponse
   * @return true if get successes.
   * @throws TiClientInternalException when @param resp is null.
   * @throws KeyException when fails to resolve lock.
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

    return handleCommitRespOnError(backOffer, resp.hasError(), resp.getError());
  }

  // TODO: batch get should consider key range split
  public List<KvPair> batchGet(BackOffer backOffer, Iterable<ByteString> keys, long version) {
    Supplier<BatchGetRequest> request =
        () ->
            BatchGetRequest.newBuilder()
                .setContext(region.getContext())
                .addAllKeys(keys)
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
    return doBatchGet(resp, backOffer);
  }

  // TODO: deal with resolve locks and region errors
  private List<KvPair> doBatchGet(BatchGetResponse resp, BackOffer bo) {
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
      }

      // TODO: we should retry
      // fix me
    }

    if (resp.hasRegionError()) {
      // TODO, we should redo the split and redo the batchGet
      throw new RegionException(resp.getRegionError());
    }
    return resp.getPairsList();
  }

  public List<KvPair> scan(
      BackOffer backOffer, ByteString startKey, long version, boolean keyOnly) {
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
    return doScan(resp, backOffer);
  }

  // TODO: remove helper and change to while style
  // needs to be fixed as batchGet
  // which we shoule retry not throw
  // exception
  private List<KvPair> doScan(ScanResponse resp, BackOffer bo) {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("ScanResponse failed without a cause");
    }

    //    return doBatchGet();
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
      }

      // TODO: we should retry
      // fix me
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    return resp.getPairsList();
  }

  public List<KvPair> scan(BackOffer backOffer, ByteString startKey, long version) {
    return scan(backOffer, startKey, version, false);
  }

  /**
   * Prewrite batch keys
   *
   * @param backOffer specifies prewrite backoffer policy.
   * @param primary specifies primary key
   * @param mutations
   * @param startTs specifies a txn's start timestamp.
   * @param lockTTL a lock's time to live.
   * @throws TiClientInternalException when resp is null.
   * @throws KeyException when fails to resolve lock.
   * @throws RegionException when region has split during prewrite operation.
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

  /** Prewrite batch keys */
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
   * Check preweite successes or not.
   * @param backOffer specifies a backoffer policy.
   * @param resp specifies prewrite response.
   * @return true if prewrite is succeeded.
   * @throws TiClientInternalException when resp is null.
   * @throws RegionException when region has split during this operation.
   * @throws KeyException when fails to resolve lock.
   */
  private boolean isPrewriteSuccess(BackOffer backOffer, PrewriteResponse resp)
      throws TiClientInternalException, KeyException, RegionException {
    if (resp == null) {
      this.regionManager.onRequestFail(region);
      throw new TiClientInternalException("PrewriteResponse failed without a cause");
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
   * @param backOffer specifies a backoffer policy.
   * @param keys will be committed.
   * @param startVersion specifies key's start version.
   * @param commitVersion specifies key's commit version.
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
   * Check Commit successes or not
   * @param backOffer specifies a backoffer policy.
   * @param resp specifies a commit response.
   * @return true if commit is succeeded.
   * @throws TiClientInternalException when resp is null
   * @throws RegionException when region has split during this operation.
   * @throws KeyException when fails to resolve lock.
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
    return handleCommitRespOnError(backOffer, resp.hasError(), resp.getError());
  }

  private boolean handleCommitRespOnError(BackOffer backOffer, boolean b, KeyError error) {
    if (b) {
      if (error.hasLocked()) {
        Lock lock = new Lock(error.getLocked());
        boolean ok =
            lockResolverClient.resolveLocks(backOffer, new ArrayList<>(Collections.singletonList(lock)));
        if (!ok) {
          backOffer.doBackOff(BoTxnLockFast, new KeyException((error.getLocked().toString())));
        }
        return false;
      } else {
        throw new KeyException(error);
      }
    }
    return true;
  }

  /**
   * Execute and retrieve theA response from TiKV server.
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
      return RangeSplitter.newSplitter(session.getRegionManager()).splitRangeByRegion(ranges);
    }

    if (response.hasLocked()) {
      Lock lock = new Lock(response.getLocked());
      logger.debug(String.format("coprocessor encounters locks: %s", lock));
      boolean ok = lockResolverClient.resolveLocks(backOffer, new ArrayList<>(
          Collections.singletonList(lock)));
      if (!ok) {
        backOffer.doBackOff(BoTxnLockFast, new LockException(lock));
      }
      // Split ranges
      return RangeSplitter.newSplitter(session.getRegionManager()).splitRangeByRegion(ranges);
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
            StreamingResponse::getFirstRegionError // TODO: handle all errors in streaming respinse
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
    private final TiSession session;

    public RegionStoreClientBuilder(
        TiConfiguration conf,
        ChannelFactory channelFactory,
        RegionManager regionManager,
        TiSession session) {
      Objects.requireNonNull(conf, "conf is null");
      Objects.requireNonNull(channelFactory, "channelFactory is null");
      Objects.requireNonNull(regionManager, "regionManager is null");
      this.conf = conf;
      this.channelFactory = channelFactory;
      this.regionManager = regionManager;
      this.session = session;
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
          conf, region, session, channelFactory, blockingStub, asyncStub, regionManager);
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
      TiSession session,
      ChannelFactory channelFactory,
      TikvBlockingStub blockingStub,
      TikvStub asyncStub,
      RegionManager regionManager) {
    super(conf, channelFactory);
    checkNotNull(region, "Region is empty");
    checkNotNull(region.getLeader(), "Leader Peer is null");
    checkArgument(region.getLeader() != null, "Leader Peer is null");
    this.regionManager = regionManager;
    this.session = session;
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
  public void close() {}

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
