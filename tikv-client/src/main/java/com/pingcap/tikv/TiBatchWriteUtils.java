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

import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.key.RowKey;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.region.TiRegion;
import java.util.ArrayList;
import java.util.List;

public class TiBatchWriteUtils {
  public static List<TiRegion> getRegionsByTable(
      TiSession session, String databaseName, String tableName) {
    ArrayList<TiRegion> regionList = new ArrayList<>();
    TiTableInfo table = session.getCatalog().getTable(databaseName, tableName);

    Key key = RowKey.createMin(table.getId());
    RowKey endRowKey = RowKey.createBeyondMax(table.getId());

    while (key.compareTo(endRowKey) < 0) {
      System.out.println(KeyUtils.formatBytes(key.toByteString()));
      TiRegion region = session.getRegionManager().getRegionByKey(key.toByteString());
      if (region == null) {
        break;
      } else {
        regionList.add(region);
        key = Key.toRawKey(region.getEndKey().toByteArray());
      }
    }

    return regionList;
  }
}
