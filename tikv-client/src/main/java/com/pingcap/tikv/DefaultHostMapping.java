/*
 * Copyright 2022 PingCAP, Inc.
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

import static com.pingcap.tikv.pd.PDUtils.addrToUrl;

import com.google.common.annotations.Beta;
import com.pingcap.tikv.util.HostMapping;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultHostMapping implements HostMapping {
  private static final String NETWORK_MAPPING_PATH = "/client/url-mapping";
  private final Client etcdClient;
  private final String networkMappingName;
  private final ConcurrentMap<String, String> hostMapping;
  private final Logger logger = LoggerFactory.getLogger(DefaultHostMapping.class);

  public DefaultHostMapping(Client etcdClient, String networkMappingName) {
    this.etcdClient = etcdClient;
    this.networkMappingName = networkMappingName;
    this.hostMapping = new ConcurrentHashMap<>();
  }

  private ByteSequence hostToNetworkMappingKey(String host) {
    String path = NETWORK_MAPPING_PATH + "/" + networkMappingName + "/" + host;
    return ByteSequence.from(path, StandardCharsets.UTF_8);
  }

  @Beta
  private String getMappedHostFromPD(String host) {
    ByteSequence hostKey = hostToNetworkMappingKey(host);
    for (int i = 0; i < 5; i++) {
      CompletableFuture<GetResponse> future = etcdClient.getKVClient().get(hostKey);
      try {
        GetResponse resp = future.get();
        List<KeyValue> kvs = resp.getKvs();
        if (kvs.size() != 1) {
          break;
        }
        return kvs.get(0).getValue().toString(StandardCharsets.UTF_8);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        logger.info("failed to get mapped Host from PD: " + host, e);
        break;
      } catch (Exception ignore) {
        // ignore
        break;
      }
    }
    return host;
  }

  public URI getMappedURI(URI uri) {
    if (networkMappingName.isEmpty()) {
      return uri;
    }
    return addrToUrl(
        hostMapping.computeIfAbsent(uri.getHost(), this::getMappedHostFromPD)
            + ":"
            + uri.getPort());
  }
}
