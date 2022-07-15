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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv;

import static com.pingcap.tikv.operation.PDErrorHandler.getRegionResponseErrorExtractor;
import static com.pingcap.tikv.pd.PDError.buildFromPdpbError;
import static org.tikv.shade.com.google.common.base.Preconditions.checkNotNull;

import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.operation.NoopHandler;
import com.pingcap.tikv.operation.PDErrorHandler;
import com.pingcap.tikv.pd.PDUtils;
import com.pingcap.tikv.util.BackOffFunction.BackOffFuncType;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ChannelFactory;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.ConverterUpstream;
import com.pingcap.tikv.util.Pair;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.HostMapping;
import org.tikv.common.TiConfiguration.ReplicaRead;
import org.tikv.common.apiversion.RequestKeyCodec;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.util.FutureObserver;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Peer;
import org.tikv.kvproto.Metapb.Region;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.PDGrpc;
import org.tikv.kvproto.PDGrpc.PDBlockingStub;
import org.tikv.kvproto.PDGrpc.PDFutureStub;
import org.tikv.kvproto.PDGrpc.PDStub;
import org.tikv.kvproto.Pdpb;
import org.tikv.kvproto.Pdpb.Error;
import org.tikv.kvproto.Pdpb.ErrorType;
import org.tikv.kvproto.Pdpb.GetAllStoresRequest;
import org.tikv.kvproto.Pdpb.GetMembersRequest;
import org.tikv.kvproto.Pdpb.GetMembersResponse;
import org.tikv.kvproto.Pdpb.GetOperatorRequest;
import org.tikv.kvproto.Pdpb.GetOperatorResponse;
import org.tikv.kvproto.Pdpb.GetRegionByIDRequest;
import org.tikv.kvproto.Pdpb.GetRegionRequest;
import org.tikv.kvproto.Pdpb.GetRegionResponse;
import org.tikv.kvproto.Pdpb.GetStoreRequest;
import org.tikv.kvproto.Pdpb.GetStoreResponse;
import org.tikv.kvproto.Pdpb.OperatorStatus;
import org.tikv.kvproto.Pdpb.RequestHeader;
import org.tikv.kvproto.Pdpb.ResponseHeader;
import org.tikv.kvproto.Pdpb.ScatterRegionRequest;
import org.tikv.kvproto.Pdpb.ScatterRegionResponse;
import org.tikv.shade.com.google.common.annotations.VisibleForTesting;
import org.tikv.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.shade.io.grpc.ManagedChannel;

public class PDClient extends AbstractGRPCClient<PDFutureStub, PDBlockingStub, PDStub>
    implements ReadOnlyPDClient {

  private static final String TIFLASH_TABLE_SYNC_PROGRESS_PATH = "/tiflash/table/sync";
  private final Logger logger = LoggerFactory.getLogger(PDClient.class);
  private RequestHeader header;
  private volatile LeaderWrapper leaderWrapper;
  private ScheduledExecutorService service;
  private ScheduledExecutorService tiflashReplicaService;
  private List<URI> pdAddrs;
  private Client etcdClient;
  private ConcurrentMap<Long, Double> tiflashReplicaMap;
  private final org.tikv.common.ReadOnlyPDClient upstreamPDClient;

  private PDClient(TiConfiguration conf, RequestKeyCodec keyCodec, ChannelFactory channelFactory) {
    super(conf, channelFactory);
    initCluster();
    this.blockingStub = getBlockingStub();
    this.asyncStub = getAsyncStub();
    // this.asyncFutureStub = getAsyncStub();
    this.upstreamPDClient = ConverterUpstream.createUpstreamPDClient(conf, keyCodec);
  }

  @Override
  public TiTimestamp getTimestamp(org.tikv.common.util.BackOffer backOffer) {
    return upstreamPDClient.getTimestamp(backOffer);
  }

  @Override
  public org.tikv.common.util.Pair<Region, Peer> getRegionByKey(
      org.tikv.common.util.BackOffer backOffer, ByteString key) {
    return new Pair<>(upstreamPDClient.getRegionByKey(backOffer, key));
  }

  @Override
  public org.tikv.common.util.Pair<Region, Peer> getRegionByID(
      org.tikv.common.util.BackOffer backOffer, long id) {
    return new Pair<>(upstreamPDClient.getRegionByID(backOffer, id));
  }

  @Override
  public List<Pdpb.Region> scanRegions(
      org.tikv.common.util.BackOffer backOffer,
      ByteString byteString,
      ByteString byteString1,
      int i) {
    return upstreamPDClient.scanRegions(backOffer, byteString, byteString1, i);
  }

  @Override
  public HostMapping getHostMapping() {
    return upstreamPDClient.getHostMapping();
  }

  @Override
  public Store getStore(org.tikv.common.util.BackOffer backOffer, long storeId) {
    return upstreamPDClient.getStore(backOffer, storeId);
  }

  @Override
  public List<Store> getAllStores(org.tikv.common.util.BackOffer backOffer) {
    return upstreamPDClient.getAllStores(backOffer);
  }

  @Override
  public ReplicaRead getReplicaRead() {
    return upstreamPDClient.getReplicaRead();
  }

  public Long getClusterId() {
    return upstreamPDClient.getClusterId();
  }

  @Override
  public RequestKeyCodec getCodec() {
    return upstreamPDClient.getCodec();
  }

  public static ReadOnlyPDClient create(
      TiConfiguration conf, RequestKeyCodec keyCodec, ChannelFactory channelFactory) {
    return createRaw(conf, keyCodec, channelFactory);
  }

  static PDClient createRaw(
      TiConfiguration conf, RequestKeyCodec keyCodec, ChannelFactory channelFactory) {
    return new PDClient(conf, keyCodec, channelFactory);
  }

  /**
   * Sends request to pd to scatter region.
   *
   * @param region represents a region info
   */
  void scatterRegion(Metapb.Region region, BackOffer backOffer) {
    Supplier<ScatterRegionRequest> request =
        () ->
            ScatterRegionRequest.newBuilder().setHeader(header).setRegionId(region.getId()).build();

    PDErrorHandler<ScatterRegionResponse> handler =
        new PDErrorHandler<>(
            r -> r.getHeader().hasError() ? buildFromPdpbError(r.getHeader().getError()) : null,
            this);

    ScatterRegionResponse resp =
        callWithRetry(backOffer, PDGrpc.getScatterRegionMethod(), request, handler);
    // TODO: maybe we should retry here, need dig into pd's codebase.
    if (resp.hasHeader() && resp.getHeader().hasError()) {
      throw new TiClientInternalException(
          String.format("failed to scatter region because %s", resp.getHeader().getError()));
    }
  }

  /**
   * wait scatter region until finish
   *
   * @param region
   */
  void waitScatterRegionFinish(Metapb.Region region, BackOffer backOffer) {
    for (; ; ) {
      GetOperatorResponse resp = getOperator(region.getId());
      if (resp != null) {
        if (isScatterRegionFinish(resp)) {
          logger.info(String.format("wait scatter region on %d is finished", region.getId()));
          return;
        } else {
          backOffer.doBackOff(
              BackOffFuncType.BoRegionMiss, new GrpcException("waiting scatter region"));
          logger.info(
              // LogDesensitization: show region key range in log
              String.format(
                  "wait scatter region %d at key %s is %s",
                  region.getId(),
                  KeyUtils.formatBytes(resp.getDesc().toByteArray()),
                  resp.getStatus().toString()));
        }
      }
    }
  }

  private GetOperatorResponse getOperator(long regionId) {
    Supplier<GetOperatorRequest> request =
        () -> GetOperatorRequest.newBuilder().setHeader(header).setRegionId(regionId).build();
    // get operator no need to handle error and no need back offer.
    return callWithRetry(
        ConcreteBackOffer.newCustomBackOff(0),
        PDGrpc.getGetOperatorMethod(),
        request,
        new NoopHandler<>());
  }

  private boolean isScatterRegionFinish(GetOperatorResponse resp) {
    // If the current operator of region is not `scatter-region`, we could assume
    // that `scatter-operator` has finished or timeout.
    boolean finished =
        !resp.getDesc().equals(ByteString.copyFromUtf8("scatter-region"))
            || resp.getStatus() != OperatorStatus.RUNNING;

    if (resp.hasHeader()) {
      ResponseHeader header = resp.getHeader();
      if (header.hasError()) {
        Error error = header.getError();
        // heartbeat may not send to PD
        if (error.getType() == ErrorType.REGION_NOT_FOUND) {
          finished = true;
        }
      }
    }
    return finished;
  }

  @Override
  public Future<Pair<Metapb.Region, Metapb.Peer>> getRegionByKeyAsync(
      BackOffer backOffer, ByteString key) {
    FutureObserver<Pair<Metapb.Region, Metapb.Peer>, GetRegionResponse> responseObserver =
        new FutureObserver<>(resp -> new Pair<>(resp.getRegion(), resp.getLeader()));
    Supplier<GetRegionRequest> request =
        () -> GetRegionRequest.newBuilder().setHeader(header).setRegionKey(key).build();
    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(getRegionResponseErrorExtractor, this);
    callAsyncWithRetry(backOffer, PDGrpc.getGetRegionMethod(), request, responseObserver, handler);
    return responseObserver.getFuture();
  }

  @Override
  public Future<Pair<Metapb.Region, Metapb.Peer>> getRegionByIDAsync(BackOffer backOffer, long id) {
    FutureObserver<Pair<Metapb.Region, Metapb.Peer>, GetRegionResponse> responseObserver =
        new FutureObserver<>(resp -> new Pair<>(resp.getRegion(), resp.getLeader()));
    Supplier<GetRegionByIDRequest> request =
        () -> GetRegionByIDRequest.newBuilder().setHeader(header).setRegionId(id).build();
    PDErrorHandler<GetRegionResponse> handler =
        new PDErrorHandler<>(getRegionResponseErrorExtractor, this);
    callAsyncWithRetry(
        backOffer, PDGrpc.getGetRegionByIDMethod(), request, responseObserver, handler);
    return responseObserver.getFuture();
  }

  private Supplier<GetStoreRequest> buildGetStoreReq(long storeId) {
    return () -> GetStoreRequest.newBuilder().setHeader(header).setStoreId(storeId).build();
  }

  private Supplier<GetAllStoresRequest> buildGetAllStoresReq() {
    return () -> GetAllStoresRequest.newBuilder().setHeader(header).build();
  }

  private <T> PDErrorHandler<GetStoreResponse> buildPDErrorHandler() {
    return new PDErrorHandler<>(
        r -> r.getHeader().hasError() ? buildFromPdpbError(r.getHeader().getError()) : null, this);
  }

  @Override
  public Future<Store> getStoreAsync(BackOffer backOffer, long storeId) {
    FutureObserver<Store, GetStoreResponse> responseObserver =
        new FutureObserver<>(GetStoreResponse::getStore);
    callAsyncWithRetry(
        backOffer,
        PDGrpc.getGetStoreMethod(),
        buildGetStoreReq(storeId),
        responseObserver,
        buildPDErrorHandler());
    return responseObserver.getFuture();
  }

  @Override
  public void close() throws InterruptedException {
    etcdClient.close();
    if (service != null) {
      service.shutdownNow();
    }
    if (tiflashReplicaService != null) {
      tiflashReplicaService.shutdownNow();
    }
  }

  @VisibleForTesting
  RequestHeader getHeader() {
    return header;
  }

  @VisibleForTesting
  LeaderWrapper getLeaderWrapper() {
    return leaderWrapper;
  }

  private GetMembersResponse doGetMembers(BackOffer backOffer, URI url) {
    while (true) {
      try {
        ManagedChannel probChan = channelFactory.getChannel(url.getHost() + ":" + url.getPort());
        PDGrpc.PDBlockingStub stub = PDGrpc.newBlockingStub(probChan);
        GetMembersRequest request =
            GetMembersRequest.newBuilder().setHeader(RequestHeader.getDefaultInstance()).build();
        GetMembersResponse resp = stub.getMembers(request);
        // check if the response contains a valid leader
        if (resp != null && resp.getLeader().getMemberId() == 0) {
          return null;
        }
        return resp;
      } catch (Exception e) {
        logger.warn("failed to get member from pd server.", e);
        backOffer.doBackOff(BackOffFuncType.BoPDRPC, e);
      }
    }
  }

  private GetMembersResponse getMembers(URI uri) {
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(BackOffer.PD_INFO_BACKOFF);
    try {
      return doGetMembers(backOffer, uri);
    } catch (Exception e) {
      // The exception message has been logged in doBackOff(). We should not report here again.
      return null;
    }
  }

  synchronized boolean switchLeader(List<String> leaderURLs) {
    if (leaderURLs.isEmpty()) {
      return false;
    }
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

      String leaderUrlStr = resp.getLeader().getClientUrlsList().get(0);
      URI leaderUrl = PDUtils.addrToUrl(leaderUrlStr);
      leaderUrlStr = leaderUrl.getHost() + ":" + leaderUrl.getPort();

      // if leader is switched, just return.
      if (checkHealth(leaderUrlStr) && switchLeader(resp.getLeader().getClientUrlsList())) {
        return;
      }
    }
    throw new TiClientInternalException(
        "already tried all address on file, but not leader found yet.");
  }

  public synchronized void updateLeaderOrForwardFollower() {
    for (URI url : this.pdAddrs) {
      // since resp is null, we need update leader's address by walking through all pd server.
      GetMembersResponse resp = getMembers(url);
      if (resp == null) {
        continue;
      }
      if (resp.getLeader().getClientUrlsList().isEmpty()) {
        continue;
      }

      String leaderUrlStr = resp.getLeader().getClientUrlsList().get(0);
      URI leaderUrl = PDUtils.addrToUrl(leaderUrlStr);
      leaderUrlStr = leaderUrl.getHost() + ":" + leaderUrl.getPort();

      if (!checkHealth(leaderUrlStr)) {
        continue;
      }

      // create new Leader
      try {
        ManagedChannel clientChannel = channelFactory.getChannel(leaderUrlStr);
        leaderWrapper =
            new LeaderWrapper(
                leaderUrlStr,
                PDGrpc.newBlockingStub(clientChannel),
                PDGrpc.newStub(clientChannel),
                System.nanoTime());
      } catch (IllegalArgumentException e) {
        logger.error("Error updating leader. " + leaderUrlStr, e);
        continue;
      }
      logger.info(String.format("Switched to new leader: %s", leaderWrapper));
      return;
    }
  }

  public void updateTiFlashReplicaStatus() {
    ByteSequence prefix =
        ByteSequence.from(TIFLASH_TABLE_SYNC_PROGRESS_PATH, StandardCharsets.UTF_8);
    for (int i = 0; i < 5; i++) {
      CompletableFuture<GetResponse> resp;
      try {
        resp =
            etcdClient.getKVClient().get(prefix, GetOption.newBuilder().withPrefix(prefix).build());
      } catch (Exception e) {
        logger.info("get tiflash table replica sync progress failed, continue checking.", e);
        continue;
      }
      GetResponse getResp;
      try {
        getResp = resp.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        continue;
      } catch (ExecutionException e) {
        throw new GrpcException("failed to update tiflash replica", e);
      }
      ConcurrentMap<Long, Double> progressMap = new ConcurrentHashMap<>();
      for (KeyValue kv : getResp.getKvs()) {
        long tableId;
        try {
          tableId =
              Long.parseLong(
                  kv.getKey().toString().substring(TIFLASH_TABLE_SYNC_PROGRESS_PATH.length()));
        } catch (Exception e) {
          logger.debug(
              "invalid tiflash table replica sync progress key. key = " + kv.getKey().toString());
          continue;
        }
        double progress;
        try {
          progress = Double.parseDouble(kv.getValue().toString());
        } catch (Exception e) {
          logger.info(
              "invalid tiflash table replica sync progress value. value = "
                  + kv.getValue().toString());
          continue;
        }
        progressMap.put(tableId, progress);
      }
      tiflashReplicaMap = progressMap;
      break;
    }
  }

  public double getTiFlashReplicaProgress(long tableId) {
    return tiflashReplicaMap.getOrDefault(tableId, 0.0);
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
    this.pdAddrs = pdAddrs;
    this.etcdClient = Client.builder().endpoints(pdAddrs).build();
    this.tiflashReplicaMap = new ConcurrentHashMap<>();
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
    tiflashReplicaService =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true).build());
    tiflashReplicaService.scheduleAtFixedRate(
        this::updateTiFlashReplicaStatus, 10, 10, TimeUnit.SECONDS);
  }

  static class LeaderWrapper {

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
}
