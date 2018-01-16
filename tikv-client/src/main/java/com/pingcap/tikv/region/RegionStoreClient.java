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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.pingcap.tidb.tipb.DAGRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.AbstractGRPCClient;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.exception.SelectException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import com.pingcap.tikv.kvproto.Kvrpcpb.*;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.kvproto.TikvGrpc;
import com.pingcap.tikv.kvproto.TikvGrpc.TikvBlockingStub;
import com.pingcap.tikv.kvproto.TikvGrpc.TikvStub;
import com.pingcap.tikv.operation.KVErrorHandler;
import com.pingcap.tikv.streaming.StreamingResponse;
import com.pingcap.tikv.util.Pair;
import io.grpc.ManagedChannel;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.pingcap.tikv.types.RequestTypes.REQ_TYPE_DAG;

// RegionStore itself is not thread-safe
public class RegionStoreClient extends AbstractGRPCClient<TikvBlockingStub, TikvStub> implements RegionErrorReceiver {
  private static final Logger logger = Logger.getLogger(RegionStoreClient.class);
  private TiRegion region;
  private TikvBlockingStub blockingStub;
  private TikvStub asyncStub;
  private final RegionManager regionManager;

  public ByteString get(ByteString key, long version) {
    Supplier<GetRequest> factory = () ->
        GetRequest.newBuilder().setContext(region.getContext()).setKey(key).setVersion(version).build();

    KVErrorHandler<GetResponse> handler =
        new KVErrorHandler<>(
            regionManager, this, region, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    GetResponse resp = callWithRetry(TikvGrpc.METHOD_KV_GET, factory, handler);
    return getHelper(resp);
  }

  public void rawPut(ByteString key, ByteString value, Context context) {
    Supplier<RawPutRequest> factory = () ->
        RawPutRequest.newBuilder().setContext(region.getContext()).setKey(key).setValue(value).build();

    KVErrorHandler<RawPutResponse> handler =
        new KVErrorHandler<>(
            regionManager, this, region, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    RawPutResponse resp = callWithRetry(TikvGrpc.METHOD_RAW_PUT, factory, handler);
  }

  public ByteString rawGet(ByteString key, Context context) {
    Supplier<RawGetRequest> factory = () ->
        RawGetRequest.newBuilder().setContext(region.getContext()).setKey(key).build();
    KVErrorHandler<RawGetResponse> handler =
        new KVErrorHandler<>(
            regionManager, this, region, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    RawGetResponse resp = callWithRetry(TikvGrpc.METHOD_RAW_GET, factory, handler);
    return resp.getValue();
  }

  public void rawDelete(ByteString key, Context context) {
    Supplier<RawDeleteRequest> factory = () ->
        RawDeleteRequest.newBuilder().setContext(context).setKey(key).build();

    KVErrorHandler<RawDeleteResponse> handler =
        new KVErrorHandler<>(
            regionManager, this, region, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    RawDeleteResponse resp = callWithRetry(TikvGrpc.METHOD_RAW_DELETE, factory, handler);
    if (resp == null) {
      this.regionManager.onRequestFail(context.getRegionId(), context.getPeer().getStoreId());
    }
  }

  private ByteString getHelper(GetResponse resp) {
    if (resp.hasError()) {
      throw new KeyException(resp.getError());
    }
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    return resp.getValue();
  }

  public List<KvPair> batchGet(Iterable<ByteString> keys, long version) {
    Supplier<BatchGetRequest> request = () ->
        BatchGetRequest.newBuilder()
            .setContext(region.getContext())
            .addAllKeys(keys)
            .setVersion(version)
            .build();
    KVErrorHandler<BatchGetResponse> handler =
        new KVErrorHandler<>(
            regionManager, this, region, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    BatchGetResponse resp = callWithRetry(TikvGrpc.METHOD_KV_BATCH_GET, request, handler);
    return batchGetHelper(resp);
  }

  private List<KvPair> batchGetHelper(BatchGetResponse resp) {
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    return resp.getPairsList();
  }

  public List<KvPair> scan(ByteString startKey, long version) {
    return scan(startKey, version, false);
  }

  public List<KvPair> scan(ByteString startKey, long version, boolean keyOnly) {
    Supplier<ScanRequest> request = () ->
        ScanRequest.newBuilder()
            .setContext(region.getContext())
            .setStartKey(startKey)
            .setVersion(version)
            .setKeyOnly(keyOnly)
            .setLimit(getConf().getScanBatchSize())
            .build();

    KVErrorHandler<ScanResponse> handler =
        new KVErrorHandler<>(
            regionManager, this, region, resp -> resp.hasRegionError() ? resp.getRegionError() : null);
    ScanResponse resp = callWithRetry(TikvGrpc.METHOD_KV_SCAN, request, handler);
    return scanHelper(resp);
  }

  private List<KvPair> scanHelper(ScanResponse resp) {
    if (resp.hasRegionError()) {
      throw new RegionException(resp.getRegionError());
    }
    return resp.getPairsList();
  }

  /**
   * Execute a DAGRequest and retrieve the response from TiKV server.
   *
   * @param req    SelectRequest to process
   * @param ranges Key range list
   * @return Execution result computed by coprocessor
   */
  public SelectResponse coprocess(DAGRequest req, List<KeyRange> ranges) {
    if (req == null ||
        ranges == null ||
        req.getExecutorsCount() < 1) {
      throw new IllegalArgumentException("Invalid coprocess argument!");
    }

    Supplier<Coprocessor.Request> reqToSend = () ->
        Coprocessor.Request.newBuilder()
            .setContext(region.getContext())
            .setTp(REQ_TYPE_DAG.getValue())
            .setData(req.toByteString())
            .addAllRanges(ranges)
            .build();
    KVErrorHandler<Coprocessor.Response> handler =
        new KVErrorHandler<>(
            regionManager, this, region, resp -> resp.hasRegionError() ? resp.getRegionError() : null,
            Coprocessor.Response::getOtherError);
    Coprocessor.Response resp = callWithRetry(TikvGrpc.METHOD_COPROCESSOR, reqToSend, handler);
    return coprocessorHelper(resp);
  }

  public Iterator<SelectResponse> coprocessStreaming(DAGRequest req, List<KeyRange> ranges) {
    Supplier<Coprocessor.Request> reqToSend = () ->
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
            StreamingResponse::getFirstRegionError,//TODO: handle all errors in streaming respinse
            StreamingResponse::getFirstOtherError
        );

    StreamingResponse responseIterator = callServerStreamingWithRetry(
        TikvGrpc.METHOD_COPROCESSOR_STREAM,
        reqToSend,
        handler);

    return coprocessorHelper(responseIterator);
  }

  private Iterator<SelectResponse> coprocessorHelper(StreamingResponse response) {
    Iterator<Coprocessor.Response> responseIterator = response.iterator();
    // If we got nothing to handle, return null
    if (!responseIterator.hasNext())
      return null;

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

  @Override
  public void close() throws Exception {
  }

  public static RegionStoreClient create(
      TiRegion region, Store store, TiSession session) {
    RegionStoreClient client;
    String addressStr = store.getAddress();
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("Create region store client on address %s", addressStr));
    }
    ManagedChannel channel = session.getChannel(addressStr);

    TikvBlockingStub blockingStub = TikvGrpc.newBlockingStub(channel);

    TikvStub asyncStub = TikvGrpc.newStub(channel);
    client =
        new RegionStoreClient(region, session, blockingStub, asyncStub);
    return client;
  }

  private RegionStoreClient(
      TiRegion region,
      TiSession session,
      TikvBlockingStub blockingStub,
      TikvStub asyncStub) {
    super(session);
    checkNotNull(region, "Region is empty");
    checkNotNull(region.getLeader(), "Leader Peer is null");
    checkArgument(region.getLeader() != null, "Leader Peer is null");
    this.regionManager = session.getRegionManager();
    this.blockingStub = blockingStub;
    this.asyncStub = asyncStub;
    this.region = region;
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
  public void onNotLeader(TiRegion newRegion, Store newStore) {
    String addressStr = newStore.getAddress();
    ManagedChannel channel = getSession().getChannel(addressStr);
    region = newRegion;
    if (!region.switchPeer(newStore.getId())) {
      throw new TiClientInternalException("Failed to switch leader");
    }
    blockingStub = TikvGrpc.newBlockingStub(channel);
    asyncStub = TikvGrpc.newStub(channel);
  }

  @Override
  public void onStoreNotMatch() {
    Pair<TiRegion, Store> regionStorePair =
        regionManager.getRegionStorePairByRegionId(region.getId());
    Store store = regionStorePair.second;
    String addressStr = store.getAddress();
    ManagedChannel channel = getSession().getChannel(addressStr);
    blockingStub = TikvGrpc.newBlockingStub(channel);
    asyncStub = TikvGrpc.newStub(channel);
    region = regionStorePair.first;
    region.switchPeer(store.getId());
  }
}
