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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.common;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.tikv.common.key.IndexKey;
import org.tikv.common.key.Key;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiIndexInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.region.TiRegion;

public class TiBatchWriteUtils {

  private static final Comparator<TiIndexInfo> tiIndexInfoComparator =
      Comparator.comparing(TiIndexInfo::getId);

  public static List<TiRegion> getRegionByIndex(
      TiSession session, TiTableInfo table, TiIndexInfo index) {
    ArrayList<TiRegion> regionList = new ArrayList<>();
    Key min = IndexKey.toIndexKey(table.getId(), index.getId());
    Key max = min.nextPrefix();

    while (min.compareTo(max) < 0) {
      TiRegion region = session.getRegionManager().getRegionByKey(min.toByteString());
      regionList.add(region);
      min = Key.toRawKey(region.getEndKey());
    }
    return regionList;
  }

  public static List<TiRegion> getIndexRegions(TiSession session, TiTableInfo table) {
    return table
        .getIndices()
        .stream()
        .sorted(tiIndexInfoComparator)
        .flatMap(index -> getRegionByIndex(session, table, index).stream())
        .collect(Collectors.toList());
  }

  public static List<TiRegion> getRecordRegions(TiSession session, TiTableInfo table) {
    ArrayList<TiRegion> regionList = new ArrayList<>();
    Key key = RowKey.createMin(table.getId());
    RowKey endRowKey = RowKey.createBeyondMax(table.getId());

    while (key.compareTo(endRowKey) < 0) {
      TiRegion region = session.getRegionManager().getRegionByKey(key.toByteString());
      regionList.add(region);
      key = Key.toRawKey(region.getEndKey());
    }
    return regionList;
  }

  public static List<TiRegion> getRegionsByTable(TiSession session, TiTableInfo table) {
    List<TiRegion> recordRegions = getIndexRegions(session, table);
    recordRegions.addAll(getRecordRegions(session, table));
    return recordRegions;
  }
}
