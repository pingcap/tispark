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

package com.pingcap.tikv.hostmap;


import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.HostMapping;

public class UriHostMapping implements HostMapping {

  private static final Logger logger = LoggerFactory.getLogger(UriHostMapping.class.getName());
  private final ConcurrentMap<String, String> hostMapping;

  public UriHostMapping(String hostMappingString) {
    if (hostMappingString == null || hostMappingString.isEmpty()) {
      hostMapping = null;
      return;
    }
    try {
      this.hostMapping =
          Arrays.stream(hostMappingString.split(";"))
              .map(
                  s -> {
                    String[] hostAndPort = s.split(":");
                    return new ConcurrentHashMap.SimpleEntry<>(hostAndPort[0], hostAndPort[1]);
                  })
              .collect(
                  Collectors.toConcurrentMap(
                      ConcurrentHashMap.SimpleEntry::getKey,
                      ConcurrentHashMap.SimpleEntry::getValue));
    } catch (Exception e) {
      logger.error("Invalid host mapping string: {}", hostMappingString, e);
      throw new IllegalArgumentException("Invalid host mapping string: " + hostMappingString);
    }
  }

  public ConcurrentMap<String, String> getHostMapping() {
    return hostMapping;
  }

  @Override
  public URI getMappedURI(URI uri) {
    if (hostMapping != null && hostMapping.containsKey(uri.getHost())) {
      try {
        return new URI(
            uri.getScheme(),
            uri.getUserInfo(),
            hostMapping.get(uri.getHost()),
            uri.getPort(),
            uri.getPath(),
            uri.getQuery(),
            uri.getFragment());
      } catch (URISyntaxException ex) {
        logger.error("Failed to get mapped URI", ex);
        throw new IllegalArgumentException(ex);
      }
    }
    return uri;
  }
}
