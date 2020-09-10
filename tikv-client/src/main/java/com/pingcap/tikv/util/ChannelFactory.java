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

package com.pingcap.tikv.util;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ChannelFactory implements AutoCloseable {
  private final int maxFrameSize;
  private final Map<String, ManagedChannel> connPool = new ConcurrentHashMap<>();

  public ChannelFactory(int maxFrameSize) {
    this.maxFrameSize = maxFrameSize;
  }

  public ManagedChannel getChannel(String addressStr) {
    return connPool.computeIfAbsent(
        addressStr,
        key -> {
          URI address;
          try {
            address = URI.create("http://" + key);
          } catch (Exception e) {
            throw new IllegalArgumentException("failed to form address " + key);
          }
          // Channel should be lazy without actual connection until first call
          // So a coarse grain lock is ok here
          return ManagedChannelBuilder.forAddress(address.getHost(), address.getPort())
              .maxInboundMessageSize(maxFrameSize)
              .usePlaintext()
              .idleTimeout(60, TimeUnit.SECONDS)
              .build();
        });
  }

  @Override
  public void close() {
    connPool.forEach(
        (k, v) -> {
          v.shutdownNow();
          try {
            v.awaitTermination(1, TimeUnit.SECONDS);
          } catch (InterruptedException ignored) {
          }
        });
    connPool.clear();
  }
}
