/*
 * Copyright 2020 PingCAP, Inc.
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

import com.pingcap.tikv.region.TiStoreType;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Metapb;

public class StoreVersion {

  private static int scale = 10000;
  private int v0 = 9999;
  private int v1 = 9999;
  private int v2 = 9999;

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private StoreVersion(String version) {
    try {
      String parts[] = version.split("[.-]");
      if (parts.length > 0) {
        v0 = Integer.parseInt(parts[0]);
      }
      if (parts.length > 1) {
        v1 = Integer.parseInt(parts[1]);
      }
      if (parts.length > 2) {
        v2 = Integer.parseInt(parts[2]);
      }
    } catch (Exception e) {
      logger.warn("invalid store version: " + version, e);
    }
  }

  private int toIntVersion() {
    return v0 * scale * scale + v1 * scale + v2;
  }

  private boolean greatThan(StoreVersion other) {
    return toIntVersion() > other.toIntVersion();
  }

  public static int compare(String v0, String v1) {
    return new StoreVersion(v0).toIntVersion() - new StoreVersion(v1).toIntVersion();
  }

  public static boolean minTiKVVersion(String version, PDClient pdClient) {
    StoreVersion storeVersion = new StoreVersion(version);

    BackOffer bo = ConcreteBackOffer.newCustomBackOff(BackOffer.PD_INFO_BACKOFF);
    List<Metapb.Store> storeList = pdClient.getAllStores(bo);

    for (Metapb.Store store : storeList) {
      if (!isTiFlash(store) && storeVersion.greatThan(new StoreVersion(store.getVersion()))) {
        return false;
      }
    }
    return true;
  }

  private static boolean isTiFlash(Metapb.Store store) {
    for (Metapb.StoreLabel label : store.getLabelsList()) {
      if (label.getKey().equals(TiStoreType.TiFlash.getLabelKey())
          && label.getValue().equals(TiStoreType.TiFlash.getLabelValue())) {
        return true;
      }
    }
    return false;
  }
}
