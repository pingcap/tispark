/*
 *
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
 *
 */

package com.pingcap.tikv.region;

import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.event.CacheInvalidateEvent;
import com.pingcap.tikv.util.ConverterUpstream;
import java.util.function.Function;
import org.tikv.common.ReadOnlyPDClient;

@SuppressWarnings("UnstableApiUsage")
public class RegionManager extends org.tikv.common.region.RegionManager {
  private final Function<CacheInvalidateEvent, Void> cacheInvalidateCallback;

  // To avoid double retrieval, we used the async version of grpc
  // When rpc not returned, instead of call again, it wait for previous one done
  public RegionManager(
      TiConfiguration conf,
      ReadOnlyPDClient pdClient,
      Function<CacheInvalidateEvent, Void> cacheInvalidateCallback) {
    super(ConverterUpstream.convertTiConfiguration(conf), pdClient);
    this.cacheInvalidateCallback = cacheInvalidateCallback;
  }

  public RegionManager(TiConfiguration conf, ReadOnlyPDClient pdClient) {
    super(ConverterUpstream.convertTiConfiguration(conf), pdClient);
    this.cacheInvalidateCallback = null;
  }

  public Function<CacheInvalidateEvent, Void> getCacheInvalidateCallback() {
    return cacheInvalidateCallback;
  }
}
