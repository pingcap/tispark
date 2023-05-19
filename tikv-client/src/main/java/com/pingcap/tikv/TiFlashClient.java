/*
 * Copyright 2023 PingCAP, Inc.
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

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.AbstractGRPCClient;
import org.tikv.common.PDClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.ChannelFactory;
import org.tikv.kvproto.Mpp;
import org.tikv.kvproto.TikvGrpc;
import org.tikv.shade.io.grpc.ManagedChannel;
import org.tikv.shade.io.grpc.stub.ClientCalls;

public class TiFlashClient
    extends AbstractGRPCClient<TikvGrpc.TikvBlockingStub, TikvGrpc.TikvFutureStub> {

  private static final Logger logger = LoggerFactory.getLogger(TiFlashClient.class);
  protected final PDClient pdClient;
  protected TiStore store;

  public TiFlashClient(
      TiConfiguration conf, TiStore store, ChannelFactory channelFactory, PDClient pdClient) {
    super(conf, channelFactory);
    this.pdClient = pdClient;
    this.store = store;
    String addressStr = store.getStore().getAddress();
    ManagedChannel channel = channelFactory.getChannel(addressStr, pdClient.getHostMapping());
    this.blockingStub = TikvGrpc.newBlockingStub(channel);
    this.asyncStub = TikvGrpc.newFutureStub(channel);
  }

  @Override
  protected TikvGrpc.TikvBlockingStub getBlockingStub() {
    return blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS);
  }

  @Override
  protected TikvGrpc.TikvFutureStub getAsyncStub() {
    return asyncStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() throws Exception {}

  public static boolean isMppAlive(ManagedChannel channel, int timeout) {
    TikvGrpc.TikvBlockingStub stub =
        TikvGrpc.newBlockingStub(channel).withDeadlineAfter(timeout, TimeUnit.MILLISECONDS);
    Supplier<Mpp.IsAliveRequest> factory = () -> Mpp.IsAliveRequest.newBuilder().build();
    try {
      Mpp.IsAliveResponse resp =
          ClientCalls.blockingUnaryCall(
              stub.getChannel(), TikvGrpc.getIsAliveMethod(), stub.getCallOptions(), factory.get());
      return resp != null && resp.getAvailable();
    } catch (Exception e) {
      logger.warn("Call mpp isAlive fail with Exception", e);
      return false;
    }
  }
}
