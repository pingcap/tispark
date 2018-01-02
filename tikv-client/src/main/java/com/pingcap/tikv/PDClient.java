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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.Codec.BytesCodec;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.kvproto.Kvrpcpb.IsolationLevel;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.kvproto.PDGrpc;
import com.pingcap.tikv.kvproto.PDGrpc.PDBlockingStub;
import com.pingcap.tikv.kvproto.PDGrpc.PDStub;
import com.pingcap.tikv.kvproto.Pdpb.*;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.operation.PDErrorHandler;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.FutureObserver;
import io.grpc.ManagedChannel;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

public class PDClient extends AbstractGRPCClient<PDBlockingStub, PDStub>
    implements ReadOnlyPDClient {
  private RequestHeader header;
  private TsoRequest tsoReq;
  private volatile LeaderWrapper leaderWrapper;
  private ScheduledExecutorService service;
  private IsolationLevel isolationLevel;
  private List<HostAndPort> pdAddrs;


  @Override
  public TiTimestamp getTimestamp() {
    Supplier<TsoRequest> request = () -> tsoReq;

    PDErrorHandler<TsoResponse> handler =
        new PDErrorHandler<>(r -> r.getHeader().hasError() ? r.getHeader().getError() : null, this);

    TsoResponse resp = callWithRetry(PDGrpc.METHOD_TSO, request, handler);
    Timestamp timestamp = resp.getTimestamp();
    return new TiTimestamp(timestamp.getPhysical(), timestamp.getLogical());
  }

  @Override
  public TiRegion getRegionByKey(ByteString key) {
    CodecDataOutput cdo = new CodecDataOutput();
    BytesCodec.writeBytes(cdo, key.toByteArray());
    ByteString encodedKey = cdo.toByteString();

    Supplier<GetRegionRequest> request = () ->
        GetRegionRequest.newBuilder().setHeader(header).setRegionKey(encodedKey).build();

    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(r -> r.getHeader().hasError() ? r.getHeader().getError() : null, this);

    GetRegionResponse resp = callWithRetry(PDGrpc.METHOD_GET_REGION, request, handler);
    return new TiRegion(resp.getRegion(), resp.getLeader(), conf.getIsolationLevel(), conf.getCommandPriority());
  }

  @Override
  public Future<TiRegion> getRegionByKeyAsync(ByteString key) {
    FutureObserver<TiRegion, GetRegionResponse> responseObserver =
        new FutureObserver<>(resp -> new TiRegion(resp.getRegion(), resp.getLeader(), conf.getIsolationLevel(), conf.getCommandPriority()));
    Supplier<GetRegionRequest> request = () ->
        GetRegionRequest.newBuilder().setHeader(header).setRegionKey(key).build();

    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(r -> r.getHeader().hasError() ? r.getHeader().getError() : null, this);
    callAsyncWithRetry(PDGrpc.METHOD_GET_REGION, request, responseObserver, handler);
    return responseObserver.getFuture();
  }

  @Override
  public TiRegion getRegionByID(long id) {
    Supplier<GetRegionByIDRequest> request = () ->
        GetRegionByIDRequest.newBuilder().setHeader(header).setRegionId(id).build();
    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(r -> r.getHeader().hasError() ? r.getHeader().getError() : null, this);
    GetRegionResponse resp = callWithRetry(PDGrpc.METHOD_GET_REGION_BY_ID, request, handler);
    // Instead of using default leader instance, explicitly set no leader to null
    return new TiRegion(resp.getRegion(), resp.getLeader(), conf.getIsolationLevel(), conf.getCommandPriority());
  }

  @Override
  public Future<TiRegion> getRegionByIDAsync(long id) {
    FutureObserver<TiRegion, GetRegionResponse> responseObserver =
        new FutureObserver<>(resp -> new TiRegion(resp.getRegion(), resp.getLeader(), conf.getIsolationLevel(), conf.getCommandPriority()));

    Supplier<GetRegionByIDRequest> request = () ->
        GetRegionByIDRequest.newBuilder().setHeader(header).setRegionId(id).build();
    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(r -> r.getHeader().hasError() ? r.getHeader().getError() : null, this);
    callAsyncWithRetry(PDGrpc.METHOD_GET_REGION_BY_ID, request, responseObserver, handler);
    return responseObserver.getFuture();
  }

  @Override
  public Store getStore(long storeId) {
    Supplier<GetStoreRequest> request = () ->
        GetStoreRequest.newBuilder().setHeader(header).setStoreId(storeId).build();
    PDErrorHandler<GetStoreResponse> handler =
        new PDErrorHandler<>(r -> r.getHeader().hasError() ? r.getHeader().getError() : null, this);
    GetStoreResponse resp = callWithRetry(PDGrpc.METHOD_GET_STORE, request, handler);
    return resp.getStore();
  }

  @Override
  public Future<Store> getStoreAsync(long storeId) {
    FutureObserver<Store, GetStoreResponse> responseObserver =
        new FutureObserver<>(GetStoreResponse::getStore);

    Supplier<GetStoreRequest> request = () ->
        GetStoreRequest.newBuilder().setHeader(header).setStoreId(storeId).build();
    PDErrorHandler<GetStoreResponse> handler =
        new PDErrorHandler<>(r -> r.getHeader().hasError() ? r.getHeader().getError() : null, this);
    callAsyncWithRetry(PDGrpc.METHOD_GET_STORE, request, responseObserver, handler);
    return responseObserver.getFuture();
  }

  @Override
  public void close() throws InterruptedException {
    if (service != null) {
      service.shutdownNow();
    }
    if (getLeaderWrapper() != null) {
      getLeaderWrapper().close();
    }
  }

  public static ReadOnlyPDClient create(TiSession session) {
    return createRaw(session);
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

    void close() {
    }
  }

  public GetMembersResponse getMembers(HostAndPort url) {
    try {
      ManagedChannel probChan = session.getChannel(url.getHostText() + ":" + url.getPort());
      PDGrpc.PDBlockingStub stub = PDGrpc.newBlockingStub(probChan);
      GetMembersRequest request =
          GetMembersRequest.newBuilder().setHeader(RequestHeader.getDefaultInstance()).build();
      return stub.getMembers(request);
    } catch (Exception e) {
      logger.warn("failed to get member from pd server.", e);
    }
    return null;
  }

  private synchronized boolean switchLeader(List<String> leaderURLs) {
    if(leaderURLs.isEmpty()) return false;
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
      URL tURL = new URL(leaderUrlStr);
      HostAndPort newLeader = HostAndPort.fromParts(tURL.getHost(), tURL.getPort());
      leaderUrlStr = newLeader.toString();
      if (leaderWrapper != null && leaderUrlStr.equals(leaderWrapper.getLeaderInfo())) {
        return true;
      }

      // toIndexKey new Leader
      ManagedChannel clientChannel = session.getChannel(leaderUrlStr);
      leaderWrapper =
        new LeaderWrapper(
            leaderUrlStr,
            PDGrpc.newBlockingStub(clientChannel),
            PDGrpc.newStub(clientChannel),
            System.nanoTime());
    } catch (MalformedURLException e) {
      logger.error("Error updating leader.", e);
      return false;
    }
    logger.info(String.format("Switched to new leader: %s", leaderWrapper));
    return true;
  }

  public void updateLeader() {
    for(HostAndPort url : this.pdAddrs) {
      // since resp is null, we need update leader's address by walking through all pd server.
      GetMembersResponse resp = getMembers(url);
      if(resp == null) {
        continue;
      }
      // if leader is switched, just return.
      if(switchLeader(resp.getLeader().getClientUrlsList())) {
        return;
      }
    }
    throw new TiClientInternalException("already tried all address on file, but not leader found yet.");
  }

  @Override
  protected PDBlockingStub getBlockingStub() {
    return leaderWrapper
        .getBlockingStub()
        .withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
  }

  @Override
  protected PDStub getAsyncStub() {
    if(leaderWrapper == null) {
      throw new GrpcException("pd may be not present");
    }
    return leaderWrapper
        .getAsyncStub()
        .withDeadlineAfter(getConf().getTimeout(), getConf().getTimeoutUnit());
  }

  private PDClient(TiSession session) {
    super(session);
  }

  private void initCluster() {
    GetMembersResponse resp = null;
    List<HostAndPort> pdAddrs = getSession().getConf().getPdAddrs();
    for(HostAndPort u: pdAddrs) {
      resp = getMembers(u);
      if(resp != null) {
        break;
      }
    }
    checkNotNull(resp, "Failed to init client for PD cluster.");
    long clusterId = resp.getHeader().getClusterId();
    header = RequestHeader.newBuilder().setClusterId(clusterId).build();
    tsoReq = TsoRequest.newBuilder().setHeader(header).build();
    this.pdAddrs = pdAddrs;
    createLeaderWrapper(resp.getLeader().getClientUrls(0));
    service = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).build());
    service.scheduleAtFixedRate(this::updateLeader, 1, 1, TimeUnit.MINUTES);
  }

  static PDClient createRaw(TiSession session) {
    PDClient client = null;
    try {
      client = new PDClient(session);
      client.initCluster();
    } catch (Exception e) {
      if (client != null) {
        try {
          client.close();
        } catch (InterruptedException ignore) {
        }
      }
    }
    return client;
  }
}
