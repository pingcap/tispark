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

import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.TiBatchWriteException;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.txn.type.GroupKeyResult;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.tikv.kvproto.Metapb;

public class Utils {
  public static GroupKeyResult groupKeysByRegion(
      RegionManager regionManager, List<ByteString> keys, BackOffer backOffer) {
    return groupKeysByRegion(
        regionManager, keys.toArray(new ByteString[0]), keys.size(), backOffer);
  }

  public static GroupKeyResult groupKeysByRegion(
      RegionManager regionManager, ByteString[] keys, int size, BackOffer backOffer)
      throws TiBatchWriteException {
    Map<Pair<TiRegion, Metapb.Store>, List<ByteString>> groups = new HashMap<>();
    int index = 0;
    try {
      for (; index < size; index++) {
        ByteString key = keys[index];
        Pair<TiRegion, Metapb.Store> pair = regionManager.getRegionStorePairByKey(key, backOffer);
        if (pair != null) {
          groups.computeIfAbsent(pair, e -> new ArrayList<>()).add(key);
        }
      }
    } catch (Exception e) {
      throw new TiBatchWriteException("Txn groupKeysByRegion error", e);
    }
    GroupKeyResult result = new GroupKeyResult();
    result.setGroupsResult(groups);
    return result;
  }
}
