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

package com.pingcap.tikv.tools;


import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.predicates.ScanBuilder.ScanPlan;
import com.pingcap.tikv.util.RangeSplitter;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RegionUtils {
  static public Map<String, Integer>
  getRegionDistribution(TiSession session, String databaseName, String tableName) {
    requireNonNull(session, "session is null");
    requireNonNull(databaseName, "databaseName is null");
    requireNonNull(tableName, "tableName is null");
    TiTableInfo table = session.getCatalog().getTable(databaseName, tableName);
    requireNonNull(table, String.format("Table not found %s.%s", databaseName, tableName));
    ScanBuilder builder = new ScanBuilder();
    ScanPlan scanPlan = builder.buildScan(ImmutableList.of(), table);
    List<RegionTask> tasks = RangeSplitter
        .newSplitter(session.getRegionManager())
        .splitRangeByRegion(scanPlan.getKeyRanges());
    Map<String, Integer> regionMap = new HashMap<>();
    for (RegionTask task : tasks) {
      regionMap.merge(task.getHost() + "_" + task.getStore().getId(), 1, Integer::sum);
    }
    return regionMap;
  }
}
