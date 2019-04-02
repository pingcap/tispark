/*
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
 */

package com.pingcap.tikv;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.pingcap.tikv.operation.PDErrorHandler.getRegionResponseErrorExtractor;
import static com.pingcap.tikv.pd.PDError.buildFromPdpbError;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.Codec.BytesCodec;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.operation.PDErrorHandler;
import com.pingcap.tikv.pd.PDUtils;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ChannelFactory;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.FutureObserver;
import io.grpc.ManagedChannel;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.PDGrpc;
import org.tikv.kvproto.PDGrpc.PDBlockingStub;
import org.tikv.kvproto.PDGrpc.PDStub;
import org.tikv.kvproto.Pdpb.GetMembersRequest;
import org.tikv.kvproto.Pdpb.GetMembersResponse;
import org.tikv.kvproto.Pdpb.GetRegionByIDRequest;
import org.tikv.kvproto.Pdpb.GetRegionRequest;
import org.tikv.kvproto.Pdpb.GetRegionResponse;
import org.tikv.kvproto.Pdpb.GetStoreRequest;
import org.tikv.kvproto.Pdpb.GetStoreResponse;
import org.tikv.kvproto.Pdpb.RequestHeader;
import org.tikv.kvproto.Pdpb.ScatterRegionRequest;
import org.tikv.kvproto.Pdpb.ScatterRegionResponse;
import org.tikv.kvproto.Pdpb.Timestamp;
import org.tikv.kvproto.Pdpb.TsoRequest;
import org.tikv.kvproto.Pdpb.TsoResponse;

public class PDClient extends AbstractGRPCClient<PDBlockingStub, PDStub>
    implements ReadOnlyPDClient {

  private RequestHeader header;
  private TsoRequest tsoReq;
  private volatile LeaderWrapper leaderWrapper;
  private ScheduledExecutorService service;
  private List<URI> pdAddrs;

  public void scatterRegion(TiRegion left) {
    Supplier<ScatterRegionRequest> request =
        () -> ScatterRegionRequest.newBuilder().setHeader(header).setRegionId(left.getId()).build();

    PDErrorHandler<ScatterRegionResponse> handler =
        new PDErrorHandler<>(
            r -> r.getHeader().hasError() ? buildFromPdpbError(r.getHeader().getError()) : null,
            this);

    ScatterRegionResponse resp =
        callWithRetry(
            ConcreteBackOffer.newGetBackOff(), PDGrpc.METHOD_SCATTER_REGION, request, handler);
    if (resp.hasHeader() && resp.getHeader().hasError()) {
      throw new TiClientInternalException(
          String.format("failed to scatter region because %s", resp.getHeader().getError()));
    }
  }

  @Override
  public TiTimestamp getTimestamp(BackOffer backOffer) {
    Supplier<TsoRequest> request = () -> tsoReq;

    PDErrorHandler<TsoResponse> handler =
        new PDErrorHandler<>(
            r -> r.getHeader().hasError() ? buildFromPdpbError(r.getHeader().getError()) : null,
            this);

    TsoResponse resp = callWithRetry(backOffer, PDGrpc.METHOD_TSO, request, handler);
    Timestamp timestamp = resp.getTimestamp();
    return new TiTimestamp(timestamp.getPhysical(), timestamp.getLogical());
  }

  @Override
  public TiRegion getRegionByKey(BackOffer backOffer, ByteString key) {
    CodecDataOutput cdo = new CodecDataOutput();
    BytesCodec.writeBytes(cdo, key.toByteArray());
    ByteString encodedKey = cdo.toByteString();

    Supplier<GetRegionRequest> request =
        () -> GetRegionRequest.newBuilder().setHeader(header).setRegionKey(encodedKey).build();

    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(getRegionResponseErrorExtractor, this);

    GetRegionResponse resp = callWithRetry(backOffer, PDGrpc.METHOD_GET_REGION, request, handler);
    return new TiRegion(
        resp.getRegion(), resp.getLeader(), conf.getIsolationLevel(), conf.getCommandPriority());
  }

  @Override
  public Future<TiRegion> getRegionByKeyAsync(BackOffer backOffer, ByteString key) {
    FutureObserver<TiRegion, GetRegionResponse> responseObserver =
        new FutureObserver<>(
            resp ->
                new TiRegion(
                    resp.getRegion(),
                    resp.getLeader(),
                    conf.getIsolationLevel(),
                    conf.getCommandPriority()));
    Supplier<GetRegionRequest> request =
        () -> GetRegionRequest.newBuilder().setHeader(header).setRegionKey(key).build();

    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(getRegionResponseErrorExtractor, this);

    callAsyncWithRetry(backOffer, PDGrpc.METHOD_GET_REGION, request, responseObserver, handler);
    return responseObserver.getFuture();
  }

  @Override
  public TiRegion getRegionByID(BackOffer backOffer, long id) {
    Supplier<GetRegionByIDRequest> request =
        () -> GetRegionByIDRequest.newBuilder().setHeader(header).setRegionId(id).build();
    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(getRegionResponseErrorExtractor, this);

    GetRegionResponse resp =
        callWithRetry(backOffer, PDGrpc.METHOD_GET_REGION_BY_ID, request, handler);
    // Instead of using default leader instance, explicitly set no leader to null
    return new TiRegion(
        resp.getRegion(), resp.getLeader(), conf.getIsolationLevel(), conf.getCommandPriority());
  }

  @Override
  public Future<TiRegion> getRegionByIDAsync(BackOffer backOffer, long id) {
    FutureObserver<TiRegion, GetRegionResponse> responseObserver =
        new FutureObserver<>(
            resp ->
                new TiRegion(
                    resp.getRegion(),
                    resp.getLeader(),
                    conf.getIsolationLevel(),
                    conf.getCommandPriority()));

    Supplier<GetRegionByIDRequest> request =
        () -> GetRegionByIDRequest.newBuilder().setHeader(header).setRegionId(id).build();
    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(getRegionResponseErrorExtractor, this);

    callAsyncWithRetry(
        backOffer, PDGrpc.METHOD_GET_REGION_BY_ID, request, responseObserver, handler);
    return responseObserver.getFuture();
  }

  private Supplier<GetStoreRequest> buildGetStroeReq(long storeId) {
    return () -> GetStoreRequest.newBuilder().setHeader(header).setStoreId(storeId).build();
  }

  private PDErrorHandler<GetStoreResponse> buildPDErrorHandler() {
    return new PDErrorHandler<>(
        r -> r.getHeader().hasError() ? buildFromPdpbError(r.getHeader().getError()) : null, this);
  }

  @Override
  public Store getStore(BackOffer backOffer, long storeId) {
    return callWithRetry(
            backOffer, PDGrpc.METHOD_GET_STORE, buildGetStroeReq(storeId), buildPDErrorHandler())
        .getStore();
  }

  @Override
  public Future<Store> getStoreAsync(BackOffer backOffer, long storeId) {
    FutureObserver<Store, GetStoreResponse> responseObserver =
        new FutureObserver<>(GetStoreResponse::getStore);

    callAsyncWithRetry(
        backOffer,
        PDGrpc.METHOD_GET_STORE,
        buildGetStroeReq(storeId),
        responseObserver,
        buildPDErrorHandler());
    return responseObserver.getFuture();
  }

  @Override
  public void close() throws InterruptedException {
    if (service != null) {
      service.shutdownNow();
      service.awaitTermination(1, TimeUnit.SECONDS);
    }
    if (channelFactory != null) {
      channelFactory.close();
    }
  }

  public static ReadOnlyPDClient create(TiConfiguration conf, ChannelFactory channelFactory) {
    return createRaw(conf, channelFactory);
  }

  @VisibleForTesting
  RequestHeader getHeader() {
    return header;
  }

  @VisibleForTesting
  LeaderWrapper getLeaderWrapper() {
    return leaderWrapper;
  }

  class LeaderWrapper {
    private final String leaderInfo;
    private final PDBlockingStub blockingStub;
    private final PDStub asyncStub;
    private final long createTime;

    LeaderWrapper(
        String leaderInfo,
        PDGrpc.PDBlockingStub blockingStub,
        PDGrpc.PDStub asyncStub,
        long createTime) {
      this.leaderInfo = leaderInfo;
      this.blockingStub = blockingStub;
      this.asyncStub = asyncStub;
      this.createTime = createTime;
    }

    String getLeaderInfo() {
      return leaderInfo;
    }

    PDBlockingStub getBlockingStub() {
      return blockingStub;
    }

    PDStub getAsyncStub() {
      return asyncStub;
    }

    long getCreateTime() {
      return createTime;
    }

    @Override
    public String toString() {
      return "[leaderInfo: " + leaderInfo + "]";
    }
  }

  private GetMembersResponse getMembers(URI url) {
    try {
      ManagedChannel probChan = channelFactory.getChannel(url.getHost() + ":" + url.getPort());
      PDGrpc.PDBlockingStub stub = PDGrpc.newBlockingStub(probChan);
      GetMembersRequest request =
          GetMembersRequest.newBuilder().setHeader(RequestHeader.getDefaultInstance()).build();
      return stub.getMembers(request);
    } catch (Exception e) {
      logger.warn("failed to get member from pd server.", e);
    }
    return null;
  }

  synchronized boolean switchLeader(List<String> leaderURLs) {
    if (leaderURLs.isEmpty()) return false;
    String leaderUrlStr = leaderURLs.get(0);
    // TODO: Why not strip protocol info on server side since grpc does not need it
    if (leaderWrapper != null && leaderUrlStr.equals(leaderWrapper.getLeaderInfo())) {
      return true;
    }
    // switch leader
    return createLeaderWrapper(leaderUrlStr);
  }

  private boolean createLeaderWrapper(String leaderUrlStr) {
    try {
      URI newLeader = PDUtils.addrToUrl(leaderUrlStr);
      leaderUrlStr = newLeader.getHost() + ":" + newLeader.getPort();
      if (leaderWrapper != null && leaderUrlStr.equals(leaderWrapper.getLeaderInfo())) {
        return true;
      }

      // create new Leader
      ManagedChannel clientChannel = channelFactory.getChannel(leaderUrlStr);
      leaderWrapper =
          new LeaderWrapper(
              leaderUrlStr,
              PDGrpc.newBlockingStub(clientChannel),
              PDGrpc.newStub(clientChannel),
              System.nanoTime());
    } catch (IllegalArgumentException e) {
      logger.error("Error updating leader. " + leaderUrlStr, e);
      return false;
    }
    logger.info(String.format("Switched to new leader: %s", leaderWrapper));
    return true;
  }

  public void updateLeader() {
    for (URI url : this.pdAddrs) {
      // since resp is null, we need update leader's address by walking through all pd server.
      GetMembersResponse resp = getMembers(url);
      if (resp == null) {
        continue;
      }
      // if leader is switched, just return.
      if (switchLeader(resp.getLeader().getClientUrlsList())) {
        return;
      }
    }
    throw new TiClientInternalException(
        "already tried all address on file, but not leader found yet.");
  }

  @Override
  protected PDBlockingStub getBlockingStub() {
    if (leaderWrapper == null) {
      throw new GrpcException("PDClient may not be initialized");
    }
    return leaderWrapper
        .getBlockingStub()
        .withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
  }

  @Override
  protected PDStub getAsyncStub() {
    if (leaderWrapper == null) {
      throw new GrpcException("PDClient may not be initialized");
    }
    return leaderWrapper
        .getAsyncStub()
        .withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
  }

  private PDClient(TiConfiguration conf, ChannelFactory channelFactory) {
    super(conf, channelFactory);
  }

  private void initCluster() {
    GetMembersResponse resp = null;
    List<URI> pdAddrs = getConf().getPdAddrs();
    for (URI u : pdAddrs) {
      resp = getMembers(u);
      if (resp != null) {
        break;
      }
    }
    checkNotNull(resp, "Failed to init client for PD cluster.");
    long clusterId = resp.getHeader().getClusterId();
    header = RequestHeader.newBuilder().setClusterId(clusterId).build();
    tsoReq = TsoRequest.newBuilder().setHeader(header).setCount(1).build();
    this.pdAddrs = pdAddrs;
    createLeaderWrapper(resp.getLeader().getClientUrls(0));
    service =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true).build());
    service.scheduleAtFixedRate(
        () -> {
          // Wrap this with a try catch block in case schedule update fails
          try {
            updateLeader();
          } catch (Exception e) {
            logger.warn("Update leader failed", e);
          }
        },
        1,
        1,
        TimeUnit.MINUTES);
  }

  static PDClient createRaw(TiConfiguration conf, ChannelFactory channelFactory) {
    PDClient client = null;
    try {
      client = new PDClient(conf, channelFactory);
      client.initCluster();
    } catch (Exception e) {
      if (client != null) {
        try {
          client.close();
        } catch (InterruptedException ignore) {
        }
      }
      throw e;
    }
    return client;
  }
}
