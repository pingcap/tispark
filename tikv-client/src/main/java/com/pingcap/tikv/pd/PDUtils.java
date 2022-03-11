/*
 * Copyright 2019 PingCAP, Inc.
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

package com.pingcap.tikv.pd;

import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.util.List;

public class PDUtils {
  public static URI addrToUrl(String addr) {
    if (addr.contains("://")) {
      return URI.create(addr);
    } else {
      return URI.create("http://" + addr);
    }
  }

  public static List<URI> addrsToUrls(String[] addrs) {
    ImmutableList.Builder<URI> urlsBuilder = new ImmutableList.Builder<>();
    for (String addr : addrs) {
      urlsBuilder.add(addrToUrl(addr));
    }
    return urlsBuilder.build();
  }
}
