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

package org.tikv.common.util;

import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.security.KeyStore;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import org.tikv.common.exception.TiKVException;

public class ChannelFactory implements AutoCloseable {
  private final int maxFrameSize;
  private final Map<String, ManagedChannel> connPool = new ConcurrentHashMap<>();
  private final SslContextBuilder sslContextBuilder;
  private static final String PUB_KEY_INFRA = "PKIX";

  public ChannelFactory(int maxFrameSize) {
    this.maxFrameSize = maxFrameSize;
    this.sslContextBuilder = null;
  }

  public ChannelFactory(
      int maxFrameSize,
      String trustCertCollectionFilePath,
      String keyCertChainFilePath,
      String keyFilePath) {
    this.maxFrameSize = maxFrameSize;
    this.sslContextBuilder =
        getSslContextBuilder(trustCertCollectionFilePath, keyCertChainFilePath, keyFilePath);
  }

  public ChannelFactory(
      int maxFrameSize,
      String jksKeyPath,
      String jksKeyPassword,
      String jksTrustPath,
      String jksTrustPassword) {
    this.maxFrameSize = maxFrameSize;
    this.sslContextBuilder =
        getSslContextBuilder(jksKeyPath, jksKeyPassword, jksTrustPath, jksTrustPassword);
  }

  private SslContextBuilder getSslContextBuilder(
      String jksKeyPath, String jksKeyPassword, String jksTrustPath, String jksTrustPassword) {
    SslContextBuilder builder = GrpcSslContexts.forClient();
    try {
      if (jksKeyPath != null && jksKeyPassword != null) {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream(jksKeyPath), jksKeyPassword.toCharArray());
        KeyManagerFactory keyManagerFactory =
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, jksKeyPassword.toCharArray());
        builder.keyManager(keyManagerFactory);
      }
      if (jksTrustPath != null && jksTrustPassword != null) {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(new FileInputStream(jksTrustPath), jksTrustPassword.toCharArray());
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(PUB_KEY_INFRA);
        trustManagerFactory.init(trustStore);
        builder.trustManager(trustManagerFactory);
      }
    } catch (Exception e) {
      throw new TiKVException("JKS SSL context builder failed", e);
    }
    return builder;
  }

  private SslContextBuilder getSslContextBuilder(
      String trustCertCollectionFilePath, String keyCertChainFilePath, String keyFilePath) {
    SslContextBuilder builder = GrpcSslContexts.forClient().protocols("TLSv1.2", "TLSv1.3");
    if (trustCertCollectionFilePath != null) {
      builder.trustManager(new File(trustCertCollectionFilePath));
    }
    if (keyCertChainFilePath != null && keyFilePath != null) {
      builder.keyManager(new File(keyCertChainFilePath), new File(keyFilePath));
    }
    return builder;
  }

  private ManagedChannel addrToChannel(String addressStr) {
    URI address;
    try {
      address = URI.create("http://" + addressStr);
    } catch (Exception e) {
      throw new IllegalArgumentException("failed to form address " + addressStr);
    }

    NettyChannelBuilder builder = null;
    try {
      builder =
          NettyChannelBuilder.forAddress(address.getHost(), address.getPort())
              .maxInboundMessageSize(maxFrameSize)
              .keepAliveWithoutCalls(true)
              .idleTimeout(60, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new TiKVException("Failed to build NettyChannelBuilder", e);
    }

    if (sslContextBuilder == null) {
      return builder.usePlaintext().build();
    } else {
      SslContext sslContext = null;
      try {
        sslContext = sslContextBuilder.build();
      } catch (SSLException e) {
        throw new TiKVException("Failed to build sslContextBuilder", e);
      }
      return builder.sslContext(sslContext).build();
    }
  }

  public synchronized ManagedChannel getChannel(String addressStr) {
    ManagedChannel channel = connPool.computeIfAbsent(addressStr, this::addrToChannel);
    if (channel.isShutdown()) {
      return connPool.put(addressStr, addrToChannel(addressStr));
    }
    return channel;
  }

  @Override
  public synchronized void close() {
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
