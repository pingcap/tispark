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

import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;

import com.pingcap.tikv.operation.ErrorHandler;
import com.pingcap.tikv.policy.RetryMaxMs.Builder;
import com.pingcap.tikv.policy.RetryPolicy;
import com.pingcap.tikv.streaming.StreamingResponse;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ChannelFactory;
import io.grpc.MethodDescriptor;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractGRPCClient<
        BlockingStubT extends AbstractStub<BlockingStubT>, StubT extends AbstractStub<StubT>>
    implements AutoCloseable {
  protected final Logger logger = LoggerFactory.getLogger(this.getClass());
  protected final ChannelFactory channelFactory;
  protected TiConfiguration conf;
  protected BlockingStubT blockingStub;
  protected StubT asyncStub;

  protected AbstractGRPCClient(TiConfiguration conf, ChannelFactory channelFactory) {
    this.conf = conf;
    this.channelFactory = channelFactory;
  }

  protected AbstractGRPCClient(
      TiConfiguration conf,
      ChannelFactory channelFactory,
      BlockingStubT blockingStub,
      StubT asyncStub) {
    this.conf = conf;
    this.channelFactory = channelFactory;
    this.blockingStub = blockingStub;
    this.asyncStub = asyncStub;
  }

  public TiConfiguration getConf() {
    return conf;
  }

  // TODO: Seems a little bit messy for lambda part
  public <ReqT, RespT> RespT callWithRetry(
      BackOffer backOffer,
      MethodDescriptor<ReqT, RespT> method,
      Supplier<ReqT> requestFactory,
      ErrorHandler<RespT> handler) {
    if (logger.isTraceEnabled()) {
      logger.trace(String.format("Calling %s...", method.getFullMethodName()));
    }
    RetryPolicy.Builder<RespT> builder = new Builder<>(backOffer);
    RespT resp =
        builder
            .create(handler)
            .callWithRetry(
                () -> {
                  BlockingStubT stub = getBlockingStub();
                  return ClientCalls.blockingUnaryCall(
                      stub.getChannel(), method, stub.getCallOptions(), requestFactory.get());
                },
                method.getFullMethodName());

    if (logger.isTraceEnabled()) {
      logger.trace(String.format("leaving %s...", method.getFullMethodName()));
    }
    return resp;
  }

  protected <ReqT, RespT> void callAsyncWithRetry(
      BackOffer backOffer,
      MethodDescriptor<ReqT, RespT> method,
      Supplier<ReqT> requestFactory,
      StreamObserver<RespT> responseObserver,
      ErrorHandler<RespT> handler) {
    logger.debug(String.format("Calling %s...", method.getFullMethodName()));

    RetryPolicy.Builder<RespT> builder = new Builder<>(backOffer);
    builder
        .create(handler)
        .callWithRetry(
            () -> {
              StubT stub = getAsyncStub();
              ClientCalls.asyncUnaryCall(
                  stub.getChannel().newCall(method, stub.getCallOptions()),
                  requestFactory.get(),
                  responseObserver);
              return null;
            },
            method.getFullMethodName());
    logger.debug(String.format("leaving %s...", method.getFullMethodName()));
  }

  <ReqT, RespT> StreamObserver<ReqT> callBidiStreamingWithRetry(
      BackOffer backOffer,
      MethodDescriptor<ReqT, RespT> method,
      StreamObserver<RespT> responseObserver,
      ErrorHandler<StreamObserver<ReqT>> handler) {
    logger.debug(String.format("Calling %s...", method.getFullMethodName()));

    RetryPolicy.Builder<StreamObserver<ReqT>> builder = new Builder<>(backOffer);
    StreamObserver<ReqT> observer =
        builder
            .create(handler)
            .callWithRetry(
                () -> {
                  StubT stub = getAsyncStub();
                  return asyncBidiStreamingCall(
                      stub.getChannel().newCall(method, stub.getCallOptions()), responseObserver);
                },
                method.getFullMethodName());
    logger.debug(String.format("leaving %s...", method.getFullMethodName()));
    return observer;
  }

  public <ReqT, RespT> StreamingResponse callServerStreamingWithRetry(
      BackOffer backOffer,
      MethodDescriptor<ReqT, RespT> method,
      Supplier<ReqT> requestFactory,
      ErrorHandler<StreamingResponse> handler) {
    logger.debug(String.format("Calling %s...", method.getFullMethodName()));

    RetryPolicy.Builder<StreamingResponse> builder = new Builder<>(backOffer);
    StreamingResponse response =
        builder
            .create(handler)
            .callWithRetry(
                () -> {
                  BlockingStubT stub = getBlockingStub();
                  return new StreamingResponse(
                      blockingServerStreamingCall(
                          stub.getChannel(), method, stub.getCallOptions(), requestFactory.get()));
                },
                method.getFullMethodName());
    logger.debug(String.format("leaving %s...", method.getFullMethodName()));
    return response;
  }

  protected abstract BlockingStubT getBlockingStub();

  protected abstract StubT getAsyncStub();
}
