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
import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoTxnLockFast;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.pingcap.tidb.tipb.DAGRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.AbstractGRPCClient;
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
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.RangeSplitter;
import io.grpc.ManagedChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;
import org.apache.log4j.Logger;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Kvrpcpb.BatchGetRequest;
import org.tikv.kvproto.Kvrpcpb.BatchGetResponse;
import org.tikv.kvproto.Kvrpcpb.Context;
import org.tikv.kvproto.Kvrpcpb.GetRequest;
import org.tikv.kvproto.Kvrpcpb.GetResponse;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.kvproto.Kvrpcpb.RawDeleteRequest;
import org.tikv.kvproto.Kvrpcpb.RawDeleteResponse;
import org.tikv.kvproto.Kvrpcpb.RawGetRequest;
import org.tikv.kvproto.Kvrpcpb.RawGetResponse;
import org.tikv.kvproto.Kvrpcpb.RawPutRequest;
import org.tikv.kvproto.Kvrpcpb.RawPutResponse;
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
  // TODO: change from public to private
  // because our test needs to implement the
  // prewrite and commit which needs to use
  // lockResolverClient, after implements the
  // write implementation of tispark, we can change
  // it to private
  @VisibleForTesting public final LockResolverClient lockResolverClient;
  private TikvBlockingStub blockingStub;
  private TikvStub asyncStub;

  public ByteString get(BackOffer backOffer, ByteString key, long version) {
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

      if (resp.hasRegionError()) {
        backOffer.doBackOff(BoRegionMiss, new RegionException(resp.getRegionError()));
        continue;
      }

      if (resp.hasError()) {
        if (resp.getError().hasLocked()) {
          Lock lock = new Lock(resp.getError().getLocked());
          boolean ok =
              lockResolverClient.resolveLocks(backOffer, new ArrayList<>(Arrays.asList(lock)));
          if (!ok) {
            // if not resolve all locks, we wait and retry
            backOffer.doBackOff(
                BoTxnLockFast, new KeyException((resp.getError().getLocked().toString())));
          }

          continue;
        } else {
          // retriable or abort
          // this should trigger Spark to retry the txn
          throw new KeyException(resp.getError());
        }
      }

      return resp.getValue();
    }
  }

  public void rawPut(BackOffer backOffer, ByteString key, ByteString value, Context context) {
    Supplier<RawPutRequest> factory =
        () ->
            RawPutRequest.newBuilder()
                .setContext(region.getContext())
                .setKey(key)
                .setValue(value)
                .build();

    KVErrorHandler<RawPutResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    RawPutResponse resp = callWithRetry(backOffer, TikvGrpc.METHOD_RAW_PUT, factory, handler);
  }

  public ByteString rawGet(BackOffer backOffer, ByteString key, Context context) {
    Supplier<RawGetRequest> factory =
        () -> RawGetRequest.newBuilder().setContext(region.getContext()).setKey(key).build();
    KVErrorHandler<RawGetResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    RawGetResponse resp = callWithRetry(backOffer, TikvGrpc.METHOD_RAW_GET, factory, handler);
    return rawGetHelper(resp);
  }

  private ByteString rawGetHelper(RawGetResponse resp) {
    String error = resp.getError();
    if (error != null && !error.isEmpty()) {
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    return resp.getValue();
  }

  public void rawDelete(BackOffer backOffer, ByteString key, Context context) {
    Supplier<RawDeleteRequest> factory =
        () -> RawDeleteRequest.newBuilder().setContext(context).setKey(key).build();

    KVErrorHandler<RawDeleteResponse> handler =
        new KVErrorHandler<>(
            regionManager,
            this,
            region,
            resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    RawDeleteResponse resp = callWithRetry(backOffer, TikvGrpc.METHOD_RAW_DELETE, factory, handler);
    if (resp == null) {
      this.regionManager.onRequestFail(context.getRegionId(), context.getPeer().getStoreId());
    }
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
    return batchGetHelper(resp, backOffer);
  }

  // TODO: deal with resolve locks and region errors
  private List<KvPair> batchGetHelper(BatchGetResponse resp, BackOffer bo) {
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

    if (locks.size() > 0) {
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

  public List<KvPair> scan(BackOffer backOffer, ByteString startKey, long version) {
    return scan(backOffer, startKey, version, false);
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
    return scanHelper(resp, backOffer);
  }

  // TODO: remove helper and change to while style
  // needs to be fixed as batchGet
  // which we shoule retry not throw
  // exception
  private List<KvPair> scanHelper(ScanResponse resp, BackOffer bo) {
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

    if (locks.size() > 0) {
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
      return RangeSplitter.newSplitter(session.getRegionManager()).splitRangeByRegion(ranges);
    }

    if (response.hasLocked()) {
      Lock lock = new Lock(response.getLocked());
      logger.debug(String.format("coprocessor encounters locks: %s", lock));
      boolean ok = lockResolverClient.resolveLocks(backOffer, new ArrayList<>(Arrays.asList(lock)));
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

    responseQueue.offer(coprocessorHelper(response));
    return null;
  }

  // TODO: wait for future fix
  // coprocessStreaming doesn't handle split error
  // future work should handle it and do the resolve
  // locks correspondingly
  public Iterator<SelectResponse> coprocessStreaming(DAGRequest req, List<KeyRange> ranges) {
    Supplier<Coprocessor.Request> reqToSend =
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
    return coprocessorHelper(responseIterator);
  }

  private Iterator<SelectResponse> coprocessorHelper(StreamingResponse response) {
    Iterator<Coprocessor.Response> responseIterator = response.iterator();
    // If we got nothing to handle, return null
    if (!responseIterator.hasNext()) return null;

    // Simply wrap it
    return new Iterator<SelectResponse>() {
      @Override
      public boolean hasNext() {
        return responseIterator.hasNext();
      }

      @Override
      public SelectResponse next() {
        return coprocessorHelper(responseIterator.next());
      }
    };
  }

  private SelectResponse coprocessorHelper(Coprocessor.Response resp) {
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

  public TiSession getSession() {
    return session;
  }

  public static RegionStoreClient create(TiRegion region, Store store, TiSession session) {
    RegionStoreClient client;
    String addressStr = store.getAddress();
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("Create region store client on address %s", addressStr));
    }
    ManagedChannel channel = session.getChannel(addressStr);

    TikvBlockingStub blockingStub = TikvGrpc.newBlockingStub(channel);

    TikvStub asyncStub = TikvGrpc.newStub(channel);
    client = new RegionStoreClient(region, session, blockingStub, asyncStub);
    return client;
  }

  private RegionStoreClient(
      TiRegion region, TiSession session, TikvBlockingStub blockingStub, TikvStub asyncStub) {
    super(session);
    checkNotNull(region, "Region is empty");
    checkNotNull(region.getLeader(), "Leader Peer is null");
    checkArgument(region.getLeader() != null, "Leader Peer is null");
    this.session = session;
    this.regionManager = session.getRegionManager();
    this.region = region;
    this.blockingStub = blockingStub;
    this.asyncStub = asyncStub;
    this.lockResolverClient = new LockResolverClient(session, this.blockingStub, this.asyncStub);
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
    TiRegion cachedRegion = getSession().getRegionManager().getRegionById(region.getId());
    // When switch leader fails or the region changed its key range,
    // it would be necessary to re-split task's key range for new region.
    if (!region.switchPeer(newStore.getId())
        || !region.getStartKey().equals(cachedRegion.getStartKey())
        || !region.getEndKey().equals(cachedRegion.getEndKey())) {
      return false;
    }
    String addressStr = newStore.getAddress();
    ManagedChannel channel = getSession().getChannel(addressStr);
    blockingStub = TikvGrpc.newBlockingStub(channel);
    asyncStub = TikvGrpc.newStub(channel);
    return true;
  }

  @Override
  public void onStoreNotMatch(Store store) {
    String addressStr = store.getAddress();
    ManagedChannel channel = getSession().getChannel(addressStr);
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
