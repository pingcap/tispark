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


import static com.pingcap.tikv.key.Key.toRawKey;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.DAGRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.kvproto.Errorpb;
import com.pingcap.tikv.kvproto.Errorpb.Error;
import com.pingcap.tikv.kvproto.Errorpb.NotLeader;
import com.pingcap.tikv.kvproto.Errorpb.ServerIsBusy;
import com.pingcap.tikv.kvproto.Errorpb.StaleEpoch;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.kvproto.Kvrpcpb.Context;
import com.pingcap.tikv.kvproto.TikvGrpc;
import com.pingcap.tikv.region.TiRegion;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class KVMockServer extends TikvGrpc.TikvImplBase {

  private int port;
  private Server server;
  private TiRegion region;
  private TreeMap<Key, ByteString> dataMap = new TreeMap<>();
  private Map<ByteString, Integer> errorMap = new HashMap<>();

  // for KV error
  public static final int ABORT = 1;
  public static final int RETRY = 2;
  // for raw client error
  public static final int NOT_LEADER = 3;
  public static final int REGION_NOT_FOUND = 4;
  public static final int KEY_NOT_IN_REGION = 5;
  public static final int STALE_EPOCH = 6;
  public static final int SERVER_IS_BUSY = 7;
  public static final int STALE_COMMAND = 8;
  public static final int STORE_NOT_MATCH = 9;
  public static final int RAFT_ENTRY_TOO_LARGE = 10;

  public int getPort() {
    return port;
  }

  public void put(ByteString key, ByteString value) {
    dataMap.put(toRawKey(key), value);
  }

  public void remove(ByteString key) {
    dataMap.remove(toRawKey(key));
  }

  public void put(String key, String value) {
    put(ByteString.copyFromUtf8(key),
        ByteString.copyFromUtf8(value));
  }

  public void put(String key, ByteString data) {
    put(ByteString.copyFromUtf8(key), data);
  }

  public void putError(String key, int code) {
    errorMap.put(ByteString.copyFromUtf8(key), code);
  }

  public void clearAllMap() {
    dataMap.clear();
    errorMap.clear();
  }

  private void verifyContext(Context context) throws Exception {
    if (context.getRegionId() != region.getId()
        || !context.getRegionEpoch().equals(region.getRegionEpoch())
        || !context.getPeer().equals(region.getLeader())) {
      throw new Exception();
    }
  }

  @Override
  public void rawGet(
      com.pingcap.tikv.kvproto.Kvrpcpb.RawGetRequest request,
      io.grpc.stub.StreamObserver<com.pingcap.tikv.kvproto.Kvrpcpb.RawGetResponse>
          responseObserver) {
    try {
      verifyContext(request.getContext());
      ByteString key = request.getKey();

      Kvrpcpb.RawGetResponse.Builder builder = Kvrpcpb.RawGetResponse.newBuilder();
      Integer errorCode = errorMap.remove(key);
      Errorpb.Error.Builder errBuilder = Errorpb.Error.newBuilder();
      if (errorCode != null) {
        setErrorInfo(errorCode, errBuilder);
        builder.setRegionError(errBuilder.build());
      } else {
        builder.setValue(dataMap.get(toRawKey(key)));
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  /** */
  public void rawPut(
      com.pingcap.tikv.kvproto.Kvrpcpb.RawPutRequest request,
      io.grpc.stub.StreamObserver<com.pingcap.tikv.kvproto.Kvrpcpb.RawPutResponse>
          responseObserver) {
    try {
      verifyContext(request.getContext());
      ByteString key = request.getKey();

      Kvrpcpb.RawPutResponse.Builder builder = Kvrpcpb.RawPutResponse.newBuilder();
      Integer errorCode = errorMap.remove(key);
      Errorpb.Error.Builder errBuilder = Errorpb.Error.newBuilder();
      if (errorCode != null) {
        setErrorInfo(errorCode, errBuilder);
        builder.setRegionError(errBuilder.build());
        //builder.setError("");
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  private void setErrorInfo(int errorCode, Errorpb.Error.Builder errBuilder) {
    if (errorCode == NOT_LEADER) {
      errBuilder.setNotLeader(Errorpb.NotLeader.getDefaultInstance());
    } else if (errorCode == REGION_NOT_FOUND) {
      errBuilder.setRegionNotFound(Errorpb.RegionNotFound.getDefaultInstance());
    } else if (errorCode == KEY_NOT_IN_REGION) {
      errBuilder.setKeyNotInRegion(Errorpb.KeyNotInRegion.getDefaultInstance());
    } else if (errorCode == STALE_EPOCH) {
      errBuilder.setStaleEpoch(Errorpb.StaleEpoch.getDefaultInstance());
    } else if (errorCode == STALE_COMMAND) {
      errBuilder.setStaleCommand(Errorpb.StaleCommand.getDefaultInstance());
    } else if (errorCode == SERVER_IS_BUSY) {
      errBuilder.setServerIsBusy(Errorpb.ServerIsBusy.getDefaultInstance());
    } else if (errorCode == STORE_NOT_MATCH) {
      errBuilder.setStoreNotMatch(Errorpb.StoreNotMatch.getDefaultInstance());
    } else if (errorCode == RAFT_ENTRY_TOO_LARGE) {
      errBuilder.setRaftEntryTooLarge(Errorpb.RaftEntryTooLarge.getDefaultInstance());
    }
  }

  /** */
  public void rawDelete(
      com.pingcap.tikv.kvproto.Kvrpcpb.RawDeleteRequest request,
      io.grpc.stub.StreamObserver<com.pingcap.tikv.kvproto.Kvrpcpb.RawDeleteResponse>
          responseObserver) {
    try {
      verifyContext(request.getContext());
      ByteString key = request.getKey();

      Kvrpcpb.RawDeleteResponse.Builder builder = Kvrpcpb.RawDeleteResponse.newBuilder();
      Integer errorCode = errorMap.remove(key);
      Errorpb.Error.Builder errBuilder = Errorpb.Error.newBuilder();
      if (errorCode != null) {
        setErrorInfo(errorCode, errBuilder);
        builder.setRegionError(errBuilder.build());
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  @Override
  public void kvGet(
      com.pingcap.tikv.kvproto.Kvrpcpb.GetRequest request,
      io.grpc.stub.StreamObserver<com.pingcap.tikv.kvproto.Kvrpcpb.GetResponse> responseObserver) {
    try {
      verifyContext(request.getContext());
      if (request.getVersion() == 0) {
        throw new Exception();
      }
      ByteString key = request.getKey();

      Kvrpcpb.GetResponse.Builder builder = Kvrpcpb.GetResponse.newBuilder();
      Integer errorCode = errorMap.remove(key);
      Kvrpcpb.KeyError.Builder errBuilder = Kvrpcpb.KeyError.newBuilder();
      if (errorCode != null) {
        if (errorCode == ABORT) {
          errBuilder.setAbort("ABORT");
        } else if (errorCode == RETRY) {
          errBuilder.setRetryable("Retry");
        }
        builder.setError(errBuilder);
      } else {
        ByteString value = dataMap.get(toRawKey(key));
        builder.setValue(value);
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  @Override
  public void kvScan(
      com.pingcap.tikv.kvproto.Kvrpcpb.ScanRequest request,
      io.grpc.stub.StreamObserver<com.pingcap.tikv.kvproto.Kvrpcpb.ScanResponse> responseObserver) {
    try {
      verifyContext(request.getContext());
      if (request.getVersion() == 0) {
        throw new Exception();
      }
      ByteString key = request.getStartKey();

      Kvrpcpb.ScanResponse.Builder builder = Kvrpcpb.ScanResponse.newBuilder();
      Error.Builder errBuilder = Error.newBuilder();
      Integer errorCode = errorMap.remove(key);
      if (errorCode != null) {
        if (errorCode == ABORT) {
          errBuilder.setServerIsBusy(Errorpb.ServerIsBusy.getDefaultInstance());
        }
        builder.setRegionError(errBuilder.build());
      } else {
        ByteString startKey = request.getStartKey();
        SortedMap<Key, ByteString> kvs = dataMap.tailMap(toRawKey(startKey));
        builder.addAllPairs(
            kvs.entrySet()
                .stream()
                .map(
                    kv ->
                        Kvrpcpb.KvPair.newBuilder()
                            .setKey(kv.getKey().toByteString())
                            .setValue(kv.getValue())
                            .build())
                .collect(Collectors.toList()));
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  @Override
  public void kvBatchGet(
      com.pingcap.tikv.kvproto.Kvrpcpb.BatchGetRequest request,
      io.grpc.stub.StreamObserver<com.pingcap.tikv.kvproto.Kvrpcpb.BatchGetResponse>
          responseObserver) {
    try {
      verifyContext(request.getContext());
      if (request.getVersion() == 0) {
        throw new Exception();
      }
      List<ByteString> keys = request.getKeysList();

      Kvrpcpb.BatchGetResponse.Builder builder = Kvrpcpb.BatchGetResponse.newBuilder();
      Error.Builder errBuilder = Error.newBuilder();
      ImmutableList.Builder<Kvrpcpb.KvPair> resultList = ImmutableList.builder();
      for (ByteString key : keys) {
        Integer errorCode = errorMap.remove(key);
        if (errorCode != null) {
          if (errorCode == ABORT) {
            errBuilder.setServerIsBusy(Errorpb.ServerIsBusy.getDefaultInstance());
          }
          builder.setRegionError(errBuilder.build());
          break;
        } else {
          ByteString value = dataMap.get(toRawKey(key));
          resultList.add(Kvrpcpb.KvPair.newBuilder().setKey(key).setValue(value).build());
        }
      }
      builder.addAllPairs(resultList.build());
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  @Override
  public void coprocessor(
      com.pingcap.tikv.kvproto.Coprocessor.Request requestWrap,
      io.grpc.stub.StreamObserver<com.pingcap.tikv.kvproto.Coprocessor.Response> responseObserver) {
    try {
      verifyContext(requestWrap.getContext());

      DAGRequest request = DAGRequest.parseFrom(requestWrap.getData());
      if (request.getStartTs() == 0) {
        throw new Exception();
      }

      List<Coprocessor.KeyRange> keyRanges = requestWrap.getRangesList();

      Coprocessor.Response.Builder builderWrap = Coprocessor.Response.newBuilder();
      SelectResponse.Builder builder = SelectResponse.newBuilder();
      com.pingcap.tikv.kvproto.Errorpb.Error.Builder errBuilder = com.pingcap.tikv.kvproto.Errorpb.Error.newBuilder();


      for (Coprocessor.KeyRange keyRange : keyRanges) {
        Integer errorCode = errorMap.remove(keyRange.getStart());
        if (errorCode != null) {
          if (STALE_EPOCH == errorCode) {
            errBuilder.setStaleEpoch(StaleEpoch.getDefaultInstance());
          } else if (NOT_LEADER == errorCode) {
            errBuilder.setNotLeader(NotLeader.getDefaultInstance());
          } else {
            errBuilder.setServerIsBusy(ServerIsBusy.getDefaultInstance());
          }
          builderWrap.setRegionError(errBuilder.build());
          break;
        } else {
          ByteString startKey = keyRange.getStart();
          SortedMap<Key, ByteString> kvs = dataMap.tailMap(toRawKey(startKey));
          builder.addAllChunks(
              kvs.entrySet()
                  .stream()
                  .filter(Objects::nonNull)
                  .filter(kv -> kv.getKey().compareTo(toRawKey(keyRange.getEnd())) <= 0)
                  .map(
                      kv ->
                          Chunk.newBuilder()
                              .setRowsData(kv.getValue())
                              .build())
                  .collect(Collectors.toList()));
        }
      }

      responseObserver.onNext(builderWrap.setData(builder.build().toByteString()).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.asRuntimeException());
    }
  }

  public int start(TiRegion region) throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }
    server = ServerBuilder.forPort(port).addService(this).build().start();

    this.region = region;
    Runtime.getRuntime().addShutdownHook(new Thread(KVMockServer.this::stop));
    return port;
  }

  public void stop() {
    if (server != null) {
      server.shutdown();
    }
  }
}
