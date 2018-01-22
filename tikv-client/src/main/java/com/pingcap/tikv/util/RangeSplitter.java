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

package com.pingcap.tikv.util;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.key.RowKey;
import com.pingcap.tikv.key.RowKey.DecodeResult;
import com.pingcap.tikv.key.RowKey.DecodeResult.Status;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.pingcap.tikv.key.Key.toRawKey;
import static com.pingcap.tikv.util.KeyRangeUtils.formatByteString;
import static com.pingcap.tikv.util.KeyRangeUtils.makeCoprocRange;

public class RangeSplitter {
  public static class RegionTask implements Serializable {
    private final TiRegion region;
    private final Metapb.Store store;
    private final List<KeyRange> ranges;
    private final String host;

    public static RegionTask newInstance(TiRegion region, Metapb.Store store, List<KeyRange> ranges) {
      return new RegionTask(region, store, ranges);
    }

    RegionTask(TiRegion region, Metapb.Store store, List<KeyRange> ranges) {
      this.region = region;
      this.store = store;
      this.ranges = ranges;
      String host = null;
      try {
        host = HostAndPort.fromString(store.getAddress()).getHostText();
      } catch (Exception ignored) {}
      this.host = host;
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

  public static RangeSplitter newSplitter(RegionManager mgr) {
    return new RangeSplitter(mgr);
  }

  private RangeSplitter(RegionManager regionManager) {
    this.regionManager = regionManager;
  }

  private final RegionManager regionManager;

  /**
   * Group by a list of handles by the handles' region id
   *
   * @param tableId Table id used for the handle
   * @param handles Handle list
   * @return <RegionId, HandleList> map
   */
  public TLongObjectHashMap<TLongArrayList> groupByHandlesByRegionId(long tableId, TLongArrayList handles) {
    TLongObjectHashMap<TLongArrayList> result = new TLongObjectHashMap<>();
    handles.sort();

    int startPos = 0;
    DecodeResult decodeResult = new DecodeResult();
    while (startPos < handles.size()) {
      long curHandle = handles.get(startPos);
      RowKey key = RowKey.toRowKey(tableId, curHandle);
      Pair<TiRegion, Metapb.Store> regionStorePair = regionManager.getRegionStorePairByKey(ByteString.copyFrom(key.getBytes()));
      byte[] endKey = regionStorePair.first.getEndKey().toByteArray();
      RowKey.tryDecodeRowKey(tableId, endKey, decodeResult);
      if (decodeResult.status == Status.MIN) {
        throw new TiExpressionException("EndKey is less than current rowKey");
      } else if (decodeResult.status == Status.MAX || decodeResult.status == Status.UNKNOWN_INF) {
        result.put(regionStorePair.first.getId(), createHandleList(startPos, handles.size(), handles));
        break;
      }

      // Region range is a close-open range
      // If region end key match exactly or slightly less than a handle,
      // that handle should be excluded from current region
      // If region end key is greater than the handle, that handle should be included
      long regionEndHandle = decodeResult.handle;
      int pos = handles.binarySearch(regionEndHandle, startPos, handles.size());

      if (pos < 0) {
        // not found in handles, pos is the next greater pos
        // [startPos, pos) all included
        pos = -(pos + 1);
      } else if (decodeResult.status == Status.GREATER) {
        // found handle and then further consider decode status
        // End key decode to a value v: regionEndHandle < v < regionEndHandle + 1
        // handle at pos included
        pos ++;
      }
      result.put(regionStorePair.first.getId(), createHandleList(startPos, pos, handles));
      // pos equals to start leads to an dead loop
      // startPos and its handle is used for searching region in PD.
      // The returning close-open range should at least include startPos's handle
      // so only if PD error and startPos is not included in current region then startPos == pos
      if (startPos >= pos) {
        throw new TiExpressionException("searchKey is not included in region returned by PD");
      }
      startPos = pos;
    }

    return result;
  }

  private TLongArrayList createHandleList(
      int startPos,
      int endPos,
      TLongArrayList handles
  ) {
    TLongArrayList result = new TLongArrayList();
    for (int i = startPos; i < endPos; i++) {
      result.add(handles.get(i));
    }
    return result;
  }

  public List<RegionTask> splitHandlesByRegion(long tableId, TLongArrayList handles) {
    // Max value for current index handle range
    ImmutableList.Builder<RegionTask> regionTasks = ImmutableList.builder();

    TLongObjectHashMap<TLongArrayList> regionHandlesMap = groupByHandlesByRegionId(tableId, handles);

    regionHandlesMap.forEachEntry((k, v) -> {
      Pair<TiRegion, Metapb.Store> regionStorePair = regionManager.getRegionStorePairByRegionId(k);
      createTask(0, v.size(), tableId, v, regionStorePair, regionTasks);
      return true;
    });

    return regionTasks.build();
  }

  private void createTask(
      int startPos,
      int endPos,
      long tableId,
      TLongArrayList handles,
      Pair<TiRegion, Metapb.Store> regionStorePair,
      ImmutableList.Builder<RegionTask> regionTasks) {
    List<KeyRange> newKeyRanges = new ArrayList<>(endPos - startPos + 1);
    long startHandle = handles.get(startPos);
    long endHandle = startHandle;
    for (int i = startPos + 1; i < endPos; i++) {
      long curHandle = handles.get(i);
      if (endHandle + 1 == curHandle) {
        endHandle = curHandle;
      } else {
        newKeyRanges.add(makeCoprocRange(
            RowKey.toRowKey(tableId, startHandle).toByteString(),
            RowKey.toRowKey(tableId, endHandle + 1).toByteString())
        );
        startHandle = curHandle;
        endHandle = startHandle;
      }
    }
    newKeyRanges.add(makeCoprocRange(
        RowKey.toRowKey(tableId, startHandle).toByteString(),
        RowKey.toRowKey(tableId, endHandle + 1).toByteString())
    );
    regionTasks.add(new RegionTask(regionStorePair.first, regionStorePair.second, newKeyRanges));
  }

  public List<RegionTask> splitRangeByRegion(List<KeyRange> keyRanges, int splitFactor) {
    List<RegionTask> tempResult = splitRangeByRegion(keyRanges);
    // rule out query within one region
    if (tempResult.size() <= 1) {
      return tempResult;
    }

    ImmutableList.Builder<RegionTask> splitTasks = ImmutableList.builder();
    for (RegionTask task : tempResult) {
      // rule out queries already split
      if (task.getRanges().size() != 1) {
        continue;
      }
      List<KeyRange> splitRange = KeyRangeUtils.split(task.getRanges().get(0), splitFactor);
      for (KeyRange range : splitRange) {
        splitTasks.add(new RegionTask(task.getRegion(), task.getStore(), ImmutableList.of(range)));
      }
    }
    return splitTasks.build();
  }

  public List<RegionTask> splitRangeByRegion(List<KeyRange> keyRanges) {
    if (keyRanges == null || keyRanges.size() == 0) {
      return ImmutableList.of();
    }

    int i = 0;
    KeyRange range = keyRanges.get(i++);
    Map<Long, List<KeyRange>> idToRange = new HashMap<>(); // region id to keyRange list
    Map<Long, Pair<TiRegion, Metapb.Store>> idToRegion = new HashMap<>();

    while (true) {
      Pair<TiRegion, Metapb.Store> regionStorePair =
          regionManager.getRegionStorePairByKey(range.getStart());

      if (regionStorePair == null) {
        throw new NullPointerException("fail to get region/store pair by key " + formatByteString(range.getStart()));
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
    for (Map.Entry<Long, List<KeyRange>> entry : idToRange.entrySet()) {
      Pair<TiRegion, Metapb.Store> regionStorePair = idToRegion.get(entry.getKey());
      resultBuilder.add(
          new RegionTask(regionStorePair.first, regionStorePair.second, entry.getValue()));
    }
    return resultBuilder.build();
  }
}
