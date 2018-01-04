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
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.codec.TableCodec.DecodeResult.Status;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.pingcap.tikv.util.KeyRangeUtils.formatByteString;
import static java.util.Objects.requireNonNull;

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

  protected final RegionManager regionManager;

  // both arguments represent right side of end points
  // so that empty is +INF
  private static int rightCompareTo(ByteString lhs, ByteString rhs) {
    requireNonNull(lhs, "lhs is null");
    requireNonNull(rhs, "rhs is null");

    // both infinite
    if (lhs.isEmpty() && rhs.isEmpty()) {
      return 0;
    }
    if (lhs.isEmpty()) {
      return 1;
    }
    if (rhs.isEmpty()) {
      return -1;
    }

    return Comparables.wrap(lhs).compareTo(Comparables.wrap(rhs));
  }

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
    TableCodec.DecodeResult decodeResult = new TableCodec.DecodeResult();
    while (startPos < handles.size()) {
      long curHandle = handles.get(startPos);
      byte[] key = TableCodec.encodeRowKeyWithHandleBytes(tableId, curHandle);
      Pair<TiRegion, Metapb.Store> regionStorePair = regionManager.getRegionStorePairByKey(ByteString.copyFrom(key));
      byte[] endKey = regionStorePair.first.getEndKey().toByteArray();
      TableCodec.tryDecodeRowKey(tableId, endKey, decodeResult);
      if (decodeResult.status == Status.MIN) {
        throw new TiClientInternalException("EndKey is less than current rowKey");
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
        throw new TiClientInternalException("searchKey is not included in region returned by PD");
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
      createTask(tableId, v, regionStorePair, regionTasks);
      return true;
    });

    return regionTasks.build();
  }

  private void createTask(
      long tableId,
      TLongArrayList handles,
      Pair<TiRegion, Store> regionStorePair,
      ImmutableList.Builder<RegionTask> regionTasks) {
    TiRegion region = regionStorePair.first;
    Store store = regionStorePair.second;
    int handleLength = handles.size();
    List<KeyRange> newKeyRanges = new ArrayList<>( handleLength + 1);
    long startHandle = handles.get(0);
    long endHandle = startHandle;
    for (int i = 1; i < handleLength; i++) {
      long curHandle = handles.get(i);
      if (endHandle + 1 == curHandle) {
        endHandle = curHandle;
      } else {
        newKeyRanges.add(KeyRangeUtils.makeCoprocRangeWithHandle(
            tableId,
            startHandle,
            endHandle + 1));
        startHandle = curHandle;
        endHandle = startHandle;
      }
    }
    newKeyRanges.add(KeyRangeUtils.makeCoprocRangeWithHandle(tableId, startHandle, endHandle + 1));
    regionTasks.add(new RegionTask(region, store, newKeyRanges));
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

      requireNonNull(regionStorePair, "fail to get region/store pair by key" + range.getStart());
      TiRegion region = regionStorePair.first;
      idToRegion.putIfAbsent(region.getId(), regionStorePair);

      // both key range is close-opened
      // initial range inside pd is guaranteed to be -INF to +INF
      if (rightCompareTo(range.getEnd(), region.getEndKey()) > 0) {
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
