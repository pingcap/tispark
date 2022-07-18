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

import static org.tikv.common.pd.PDError.buildFromPdpbError;

import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.util.ChannelFactory;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.ConverterUpstream;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.HostMapping;
import org.tikv.common.ReadOnlyPDClient;
import org.tikv.common.TiConfiguration.ReplicaRead;
import org.tikv.common.apiversion.RequestKeyCodec;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.operation.ErrorHandler;
import org.tikv.common.operation.NoopHandler;
import org.tikv.common.util.BackOffFunction.BackOffFuncType;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Peer;
import org.tikv.kvproto.Metapb.Region;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.PDGrpc;
import org.tikv.kvproto.Pdpb;
import org.tikv.kvproto.Pdpb.Error;
import org.tikv.kvproto.Pdpb.ErrorType;
import org.tikv.kvproto.Pdpb.GetOperatorRequest;
import org.tikv.kvproto.Pdpb.GetOperatorResponse;
import org.tikv.kvproto.Pdpb.OperatorStatus;
import org.tikv.kvproto.Pdpb.RequestHeader;
import org.tikv.kvproto.Pdpb.ResponseHeader;
import org.tikv.kvproto.Pdpb.ScatterRegionRequest;
import org.tikv.kvproto.Pdpb.ScatterRegionResponse;
import org.tikv.shade.com.google.protobuf.ByteString;

public class PDClient implements ReadOnlyPDClient, AutoCloseable {

  private final Logger logger = LoggerFactory.getLogger(PDClient.class);

  private RequestHeader header;

  private org.tikv.common.PDClient upstreamPDClient;

  private PDClient(TiConfiguration conf, RequestKeyCodec keyCodec, ChannelFactory channelFactory) {
    this.upstreamPDClient =
        (org.tikv.common.PDClient) ConverterUpstream.createUpstreamPDClient(conf, keyCodec);
    this.header = RequestHeader.newBuilder().setClusterId(upstreamPDClient.getClusterId()).build();
  }

  @Override
  public TiTimestamp getTimestamp(org.tikv.common.util.BackOffer backOffer) {
    return upstreamPDClient.getTimestamp(backOffer);
  }

  @Override
  public Pair<Region, Peer> getRegionByKey(BackOffer backOffer, ByteString key) {
    return upstreamPDClient.getRegionByKey(backOffer, key);
  }

  @Override
  public Pair<Region, Peer> getRegionByID(BackOffer backOffer, long id) {
    return upstreamPDClient.getRegionByID(backOffer, id);
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

    ErrorHandler<ScatterRegionResponse> handler =
        new org.tikv.common.operation.PDErrorHandler<>(
            r -> r.getHeader().hasError() ? buildFromPdpbError(r.getHeader().getError()) : null,
            this.upstreamPDClient);

    ScatterRegionResponse resp =
        this.upstreamPDClient.callWithRetry(
            backOffer, PDGrpc.getScatterRegionMethod(), request, handler);
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
    return this.upstreamPDClient.callWithRetry(
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
  public void close() throws InterruptedException {
    this.upstreamPDClient.close();
  }

  public synchronized void updateLeaderOrForwardFollower() {
    this.upstreamPDClient.updateLeaderOrForwardFollower();
  }
}
