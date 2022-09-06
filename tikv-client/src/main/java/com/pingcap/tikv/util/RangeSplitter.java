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

import static com.pingcap.tikv.key.Key.toRawKey;
import static org.tikv.common.util.KeyRangeUtils.makeCoprocRange;

import com.pingcap.tikv.handle.Handle;
import com.pingcap.tikv.key.RowKey;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.region.TiStoreType;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.FastByteComparisons;
import org.tikv.common.util.Pair;
import org.tikv.common.util.RangeSplitter.RegionTask;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.shade.com.google.common.collect.ImmutableList;
import org.tikv.shade.com.google.protobuf.ByteString;

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
  public Map<Pair<TiRegion, TiStore>, List<Handle>> groupByAndSortHandlesByRegionId(
      long tableId, List<Handle> handles) {
    TLongObjectHashMap<List<Handle>> regionHandles = new TLongObjectHashMap<>();
    TLongObjectHashMap<Pair<TiRegion, TiStore>> idToRegionStorePair = new TLongObjectHashMap<>();
    Map<Pair<TiRegion, TiStore>, List<Handle>> result = new HashMap<>();
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
        Pair<TiRegion, TiStore> regionStorePair =
            regionManager.getRegionStorePairByKey(ByteString.copyFrom(key.getBytes()));
        curRegion = regionStorePair.first;
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
          Pair<TiRegion, TiStore> regionStorePair = idToRegionStorePair.get(k);
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

    Map<Pair<TiRegion, TiStore>, List<Handle>> regionHandlesMap =
        groupByAndSortHandlesByRegionId(tableId, handles);

    regionHandlesMap.forEach((k, v) -> createTask(0, v.size(), tableId, v, k, regionTasks));

    return regionTasks.build();
  }

  private void createTask(
      int startPos,
      int endPos,
      long tableId,
      List<Handle> handles,
      Pair<TiRegion, TiStore> regionStorePair,
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
    regionTasks.add(
        RegionTask.newInstance(regionStorePair.first, regionStorePair.second, newKeyRanges));
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

    int i = 0;
    KeyRange range = keyRanges.get(i++);
    Map<Long, List<KeyRange>> idToRange = new HashMap<>(); // region id to keyRange list
    Map<Long, Pair<TiRegion, TiStore>> idToRegion = new HashMap<>();

    while (true) {
      Pair<TiRegion, TiStore> regionStorePair = null;

      BackOffer bo = ConcreteBackOffer.newGetBackOff(BackOffer.GET_MAX_BACKOFF);
      while (regionStorePair == null) {
        try {
          regionStorePair = regionManager.getRegionStorePairByKey(range.getStart(), storeType, bo);

          if (regionStorePair == null) {
            throw new NullPointerException(
                "fail to get region/store pair by key " + formatByteString(range.getStart()));
          }

          // TODO: cherry-pick https://github.com/pingcap/tispark/pull/1380 to client-java and flush
          // cache.
          if (regionStorePair.second == null) {
            LOG.warn("Cannot find valid store on " + storeType);
            regionStorePair = null;
          }
        } catch (Exception e) {
          LOG.warn("getRegionStorePairByKey error", e);
          bo.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        }
      }

      TiRegion region = regionStorePair.first;
      idToRegion.putIfAbsent(region.getId(), regionStorePair);

      // both key range is close-opened
      // initial range inside PD is guaranteed to be -INF to +INF
      // Both keys are at right hand side and then always not -INF
      if (toRawKey(range.getEnd()).compareTo(toRawKey(region.getEndKey())) > 0) {
        // current region does not cover current end key
        KeyRange cutRange =
            KeyRange.newBuilder().setStart(range.getStart()).setEnd(region.getEndKey()).build();

        List<KeyRange> ranges = idToRange.computeIfAbsent(region.getId(), k -> new ArrayList<>());
        ranges.add(cutRange);

        // cut new remaining for current range
        range = KeyRange.newBuilder().setStart(region.getEndKey()).setEnd(range.getEnd()).build();
      } else {
        // current range covered by region
        List<KeyRange> ranges = idToRange.computeIfAbsent(region.getId(), k -> new ArrayList<>());
        ranges.add(range);
        if (i >= keyRanges.size()) {
          break;
        }
        range = keyRanges.get(i++);
      }
    }

    ImmutableList.Builder<RegionTask> resultBuilder = ImmutableList.builder();
    idToRange.forEach(
        (k, v) -> {
          Pair<TiRegion, TiStore> regionStorePair = idToRegion.get(k);
          resultBuilder.add(
              RegionTask.newInstance(regionStorePair.first, regionStorePair.second, v));
        });
    return resultBuilder.build();
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

  static String formatByteString(ByteString key) {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < key.size(); ++i) {
      sb.append(key.byteAt(i) & 255);
      if (i < key.size() - 1) {
        sb.append(",");
      }
    }

    return sb.toString();
  }
}
