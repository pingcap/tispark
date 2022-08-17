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

package com.pingcap.tikv.util;

import static com.pingcap.tikv.util.KeyRangeUtils.formatByteString;
import static com.pingcap.tikv.util.KeyRangeUtils.makeCoprocRange;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.key.Handle;
import com.pingcap.tikv.key.RowKey;
import com.pingcap.tikv.pd.PDUtils;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.RegionManager.RegionStorePair;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.region.TiStoreType;
import com.pingcap.tikv.util.BackOffFunction.BackOffFuncType;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Metapb;

public class RangeSplitter {
  private final RegionManager regionManager;

  private static final Logger LOG = LoggerFactory.getLogger(RangeSplitter.class);

  private RangeSplitter(RegionManager regionManager) {
    this.regionManager = regionManager;
  }

  public static RangeSplitter newSplitter(RegionManager mgr) {
    return new RangeSplitter(mgr);
  }

  /**
   * Group by a list of handles by the handles' region, handles will be sorted.
   *
   * @param tableId Table id used for the handle
   * @param handles Handle list
   * @return <Region, HandleList> map
   */
  public Map<RegionStorePair, List<Handle>> groupByAndSortHandlesByRegionId(
      long tableId, List<Handle> handles) {
    TLongObjectHashMap<List<Handle>> regionHandles = new TLongObjectHashMap<>();
    TLongObjectHashMap<RegionStorePair> idToRegionStorePair = new TLongObjectHashMap<>();
    Map<RegionStorePair, List<Handle>> result = new HashMap<>();
    handles.sort(Handle::compare);

    byte[] endKey = null;
    TiRegion curRegion = null;
    List<Handle> handlesInCurRegion = new ArrayList<>();
    for (Handle curHandle : handles) {
      RowKey key = RowKey.toRowKey(tableId, curHandle);
      if (endKey == null
          || (endKey.length != 0 && FastByteComparisons.compareTo(key.getBytes(), endKey) >= 0)) {
        if (curRegion != null) {
          regionHandles.put(curRegion.getId(), handlesInCurRegion);
          handlesInCurRegion = new ArrayList<>();
        }
        RegionStorePair regionStorePair =
            regionManager.getRegionTiKVStorePairByKey(ByteString.copyFrom(key.getBytes()));
        curRegion = regionStorePair.region;
        idToRegionStorePair.put(curRegion.getId(), regionStorePair);
        endKey = curRegion.getEndKey().toByteArray();
      }
      handlesInCurRegion.add(curHandle);
    }
    if (!handlesInCurRegion.isEmpty()) {
      regionHandles.put(curRegion.getId(), handlesInCurRegion);
    }
    regionHandles.forEachEntry(
        (k, v) -> {
          RegionStorePair regionStorePair = idToRegionStorePair.get(k);
          result.put(regionStorePair, v);
          return true;
        });
    return result;
  }

  public List<RegionTask> splitAndSortHandlesByRegion(List<Long> ids, List<Handle> handles) {
    Set<RegionTask> regionTasks = new HashSet<>();
    for (Long id : ids) {
      regionTasks.addAll(splitAndSortHandlesByRegion(id, handles));
    }
    return new ArrayList<>(regionTasks);
  }

  /**
   * Build region tasks from handles split by region, handles will be sorted.
   *
   * @param tableId Table ID
   * @param handles Handle list
   * @return A list of region tasks
   */
  private List<RegionTask> splitAndSortHandlesByRegion(long tableId, List<Handle> handles) {
    // Max value for current index handle range
    ImmutableList.Builder<RegionTask> regionTasks = ImmutableList.builder();

    Map<RegionStorePair, List<Handle>> regionHandlesMap =
        groupByAndSortHandlesByRegionId(tableId, handles);

    regionHandlesMap.forEach((k, v) -> createTask(0, v.size(), tableId, v, k, regionTasks));

    return regionTasks.build();
  }

  private void createTask(
      int startPos,
      int endPos,
      long tableId,
      List<Handle> handles,
      RegionStorePair regionStorePair,
      ImmutableList.Builder<RegionTask> regionTasks) {
    List<KeyRange> newKeyRanges = new ArrayList<>(endPos - startPos + 1);
    Handle startHandle = handles.get(startPos);
    Handle endHandle = startHandle;
    for (int i = startPos + 1; i < endPos; i++) {
      Handle curHandle = handles.get(i);
      if (endHandle.next().equals(curHandle)) {
        endHandle = curHandle;
      } else {
        newKeyRanges.add(
            makeCoprocRange(
                RowKey.toRowKey(tableId, startHandle).toByteString(),
                RowKey.toRowKey(tableId, endHandle.next()).toByteString()));
        startHandle = curHandle;
        endHandle = startHandle;
      }
    }
    newKeyRanges.add(
        makeCoprocRange(
            RowKey.toRowKey(tableId, startHandle).toByteString(),
            RowKey.toRowKey(tableId, endHandle.next()).toByteString()));
    regionTasks.add(new RegionTask(regionStorePair.region, regionStorePair.store, newKeyRanges));
  }

  /**
   * Split key ranges into corresponding region tasks and group by their region id
   *
   * @param keyRanges List of key ranges
   * @param storeType Store type, null or TiKV for TiKV(leader), otherwise TiFlash(learner)
   * @return List of RegionTask, each task corresponds to a different region.
   */
  public List<RegionTask> splitRangeByRegion(List<KeyRange> keyRanges, TiStoreType storeType) {
    if (keyRanges == null || keyRanges.size() == 0) {
      return ImmutableList.of();
    }
    Map<Long, List<KeyRange>> idToRange = new HashMap<>(); // region id to keyRange list
    Map<Long, RegionStorePair> idToRegion = new HashMap<>();
    BackOffer bo = ConcreteBackOffer.newGetBackOff();
    for (KeyRange range : keyRanges) {
      while (true) {
        try {
          List<RegionStorePair> regionStorePairList =
              regionManager.getAllRegionStorePairsInRange(range, storeType, bo);
          for (RegionStorePair regionStorePair : regionStorePairList) {
            TiRegion region = regionStorePair.region;
            // Add region id to RegionStorePair Map.
            idToRegion.putIfAbsent(region.getId(), regionStorePair);
          }
          extractScanRangeForRegion(idToRange, range, regionStorePairList);
          break;
        } catch (Exception e) {
          LOG.warn("getAllRegionStorePairsInRange error", e);
          bo.doBackOff(BackOffFuncType.BoRegionMiss, e);
        }
      }
    }
    ImmutableList.Builder<RegionTask> resultBuilder = ImmutableList.builder();
    idToRange.forEach(
        (k, v) -> {
          RegionStorePair regionStorePair = idToRegion.get(k);
          resultBuilder.add(new RegionTask(regionStorePair.region, regionStorePair.store, v));
        });
    return resultBuilder.build();
  }

  private static void extractScanRangeForRegion(
      Map<Long, List<KeyRange>> idToRange,
      KeyRange range,
      List<RegionStorePair> regionStorePairList) {
    if (regionStorePairList.size() == 0) {
      throw new TiClientInternalException(
          String.format(
              "fail to get region/store pairs in [ %s, %s ) ", range.getStart(), range.getEnd()));
    }

    // Both key range is close-opened.
    // Initial region range inside PD is guaranteed to be -INF to +INF.
    // Both keys are at right hand side and always not -INF.

    if (regionStorePairList.size() == 1) {
      // If only one region is returned, the range is covered by the region.
      // So the scan range is same as range.
      TiRegion region = regionStorePairList.get(0).region;
      idToRange.computeIfAbsent(region.getId(), k -> new ArrayList<>()).add(range);
      return;
    }

    // regionStorePairList.size() >= 2

    // the first region`s startKey is smaller or equal than the range`s start.
    // So the first scan range is [ range.getStart(), firstRegion.getEndKey() ).
    TiRegion firstRegion = regionStorePairList.get(0).region;
    KeyRange firstScanRange =
        KeyRange.newBuilder().setStart(range.getStart()).setEnd(firstRegion.getEndKey()).build();
    idToRange.computeIfAbsent(firstRegion.getId(), k -> new ArrayList<>()).add(firstScanRange);

    for (int i = 1; i < regionStorePairList.size() - 1; i++) {
      // The region which is the last region will necessarily be covered by the range.
      // So the middle scan range is [ region.getStartKey(), region.getEndKey() ).
      TiRegion region = regionStorePairList.get(i).region;
      KeyRange midScanRange =
          KeyRange.newBuilder().setStart(region.getStartKey()).setEnd(region.getEndKey()).build();
      idToRange.computeIfAbsent(region.getId(), k -> new ArrayList<>()).add(midScanRange);
    }

    // the last region`s endKey is greater or equal than range`s end.
    // So the last scan range is [ lastRegion.getStartKey(), range.getEnd() ).
    TiRegion lastRegion = regionStorePairList.get(regionStorePairList.size() - 1).region;
    KeyRange lastScanRange =
        KeyRange.newBuilder().setStart(lastRegion.getStartKey()).setEnd(range.getEnd()).build();
    idToRange.computeIfAbsent(lastRegion.getId(), k -> new ArrayList<>()).add(lastScanRange);
  }

  /**
   * Split key ranges into corresponding region tasks and group by their region id
   *
   * @param keyRanges List of key ranges
   * @return List of RegionTask, each task corresponds to a different region.
   */
  public List<RegionTask> splitRangeByRegion(List<KeyRange> keyRanges) {
    return splitRangeByRegion(keyRanges, TiStoreType.TiKV);
  }

  public static class RegionTask implements Serializable {
    private final TiRegion region;
    private final Metapb.Store store;
    private final List<KeyRange> ranges;
    private final String host;

    RegionTask(TiRegion region, Metapb.Store store, List<KeyRange> ranges) {
      this.region = region;
      this.store = store;
      this.ranges = ranges;
      String host = null;
      try {
        host = PDUtils.addrToUrl(store.getAddress()).getHost();
      } catch (Exception ignored) {
      }
      this.host = host;
    }

    public static RegionTask newInstance(
        TiRegion region, Metapb.Store store, List<KeyRange> ranges) {
      return new RegionTask(region, store, ranges);
    }

    public TiRegion getRegion() {
      return region;
    }

    public Metapb.Store getStore() {
      return store;
    }

    public List<KeyRange> getRanges() {
      return ranges;
    }

    public String getHost() {
      return host;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("Region [%s]", region));
      sb.append(" ");

      for (KeyRange range : ranges) {
        sb.append(
            String.format(
                "Range Start: [%s] Range End: [%s]",
                formatByteString(range.getStart()), formatByteString(range.getEnd())));
      }

      return sb.toString();
    }
  }
}
