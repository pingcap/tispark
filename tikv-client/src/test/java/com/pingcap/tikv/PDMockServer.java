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

import com.google.protobuf.ByteString;
import com.pingcap.tikv.key.Key;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingDeque;
import org.tikv.kvproto.Metapb.Region;
import org.tikv.kvproto.PDGrpc;
import org.tikv.kvproto.Pdpb;
import org.tikv.kvproto.Pdpb.GetMembersRequest;
import org.tikv.kvproto.Pdpb.GetMembersResponse;
import org.tikv.kvproto.Pdpb.GetRegionByIDRequest;
import org.tikv.kvproto.Pdpb.GetRegionRequest;
import org.tikv.kvproto.Pdpb.GetRegionResponse;
import org.tikv.kvproto.Pdpb.GetStoreRequest;
import org.tikv.kvproto.Pdpb.GetStoreResponse;
import org.tikv.kvproto.Pdpb.ResponseHeader;
import org.tikv.kvproto.Pdpb.ScanRegionsRequest;
import org.tikv.kvproto.Pdpb.ScanRegionsResponse;
import org.tikv.kvproto.Pdpb.ScanRegionsResponse.Builder;
import org.tikv.kvproto.Pdpb.TsoRequest;
import org.tikv.kvproto.Pdpb.TsoResponse;

public class PDMockServer extends PDGrpc.PDImplBase {
  private final Deque<java.util.Optional<GetMembersResponse>> getMembersResp =
      new LinkedBlockingDeque<java.util.Optional<GetMembersResponse>>();
  private final Deque<GetRegionResponse> getRegionResp = new LinkedBlockingDeque<>();
  private final Deque<GetRegionResponse> getRegionByIDResp = new LinkedBlockingDeque<>();
  private final Deque<Optional<GetStoreResponse>> getStoreResp = new LinkedBlockingDeque<>();
  public int port;
  private long clusterId;
  private Server server;

  public void addGetMemberResp(GetMembersResponse r) {
    getMembersResp.addLast(Optional.ofNullable(r));
  }

  @Override
  public void getMembers(GetMembersRequest request, StreamObserver<GetMembersResponse> resp) {
    try {
      resp.onNext(getMembersResp.getFirst().get());
      resp.onCompleted();
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  @Override
  public StreamObserver<TsoRequest> tso(StreamObserver<TsoResponse> resp) {
    return new StreamObserver<TsoRequest>() {
      private int physical = 1;
      private int logical = 0;

      @Override
      public void onNext(TsoRequest value) {}

      @Override
      public void onError(Throwable t) {}

      @Override
      public void onCompleted() {
        resp.onNext(GrpcUtils.makeTsoResponse(clusterId, physical++, logical++));
        resp.onCompleted();
      }
    };
  }

  public void addGetRegionResp(GetRegionResponse r) {
    getRegionResp.addLast(r);
  }

  @Override
  public void getRegion(GetRegionRequest request, StreamObserver<GetRegionResponse> resp) {
    try {
      resp.onNext(getRegionResp.removeFirst());
      resp.onCompleted();
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  void addGetRegionByIDResp(GetRegionResponse r) {
    getRegionByIDResp.addLast(r);
  }

  @Override
  public void getRegionByID(GetRegionByIDRequest request, StreamObserver<GetRegionResponse> resp) {
    try {
      resp.onNext(getRegionByIDResp.removeFirst());
      resp.onCompleted();
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  @Override
  public void scanRegions(ScanRegionsRequest request, StreamObserver<ScanRegionsResponse> resp) {
    try {
      ByteString startKey = request.getStartKey();
      ByteString endKey = request.getEndKey();
      int limit = request.getLimit();
      int size = 0;
      Builder builder = ScanRegionsResponse.newBuilder();
      for (GetRegionResponse response : getRegionResp) {
        Region region = response.getRegion();
        if (Key.toRawKey(endKey).compareTo(Key.toRawKey(region.getStartKey())) > 0
            || Key.toRawKey(startKey).compareTo(Key.toRawKey(region.getEndKey())) < 0) {
          Pdpb.Region respRegion =
              Pdpb.Region.newBuilder()
                  .setRegion(response.getRegion())
                  .setLeader(response.getLeader())
                  .build();
          builder.addRegions(respRegion);
          size++;
          if (size > limit) {
            break;
          }
        }
      }
      ResponseHeader defaultHeader = ResponseHeader.newBuilder().setClusterId(clusterId).build();
      builder.setHeader(defaultHeader);
      resp.onNext(builder.build());
      resp.onCompleted();
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  public void addGetStoreResp(GetStoreResponse r) {
    getStoreResp.addLast(Optional.ofNullable(r));
  }

  public void getStore(GetStoreRequest request, StreamObserver<GetStoreResponse> resp) {
    try {
      resp.onNext(getStoreResp.removeFirst().get());
      resp.onCompleted();
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  @Override
  public void getAllStores(
      org.tikv.kvproto.Pdpb.GetAllStoresRequest request,
      io.grpc.stub.StreamObserver<org.tikv.kvproto.Pdpb.GetAllStoresResponse> responseObserver) {
    try {
      responseObserver.onNext(null);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  public void start(long clusterId) throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }
    this.clusterId = clusterId;
    server = ServerBuilder.forPort(port).addService(this).build().start();

    Runtime.getRuntime().addShutdownHook(new Thread(PDMockServer.this::stop));
  }

  void stop() {
    if (server != null) {
      server.shutdownNow();
    }
  }

  public long getClusterId() {
    return clusterId;
  }
}
