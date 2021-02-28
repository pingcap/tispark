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

package com.pingcap.tikv.util;

import static com.pingcap.tikv.GrpcUtils.encodeKey;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.key.Handle;
import com.pingcap.tikv.key.IntHandle;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.key.RowKey;
import com.pingcap.tikv.key.RowKey.DecodeResult.Status;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.region.TiStoreType;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Kvrpcpb.CommandPri;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Peer;

public class RangeSplitterTest {
  private static KeyRange keyRange(Long s, Long e) {
    ByteString sKey = ByteString.EMPTY;
    ByteString eKey = ByteString.EMPTY;
    if (s != null) {
      CodecDataOutput cdo = new CodecDataOutput();
      IntegerCodec.writeLongFully(cdo, s, true);
      sKey = cdo.toByteString();
    }

    if (e != null) {
      CodecDataOutput cdo = new CodecDataOutput();
      IntegerCodec.writeLongFully(cdo, e, true);
      eKey = cdo.toByteString();
    }

    return KeyRange.newBuilder().setStart(sKey).setEnd(eKey).build();
  }

  private static ByteString shiftByStatus(ByteString v, Status s) {
    switch (s) {
      case EQUAL:
        return v;
      case LESS:
        return v.substring(0, v.size() - 1);
      case GREATER:
        return v.concat(ByteString.copyFrom(new byte[] {1, 0}));
      default:
        throw new IllegalArgumentException("Only EQUAL,LESS,GREATER allowed");
    }
  }

  private static KeyRange keyRangeByHandle(long tableId, Long s, Status ss, Long e, Status es) {
    ByteString sKey = shiftByStatus(handleToByteString(tableId, s), ss);
    ByteString eKey = shiftByStatus(handleToByteString(tableId, e), es);

    return KeyRange.newBuilder().setStart(sKey).setEnd(eKey).build();
  }

  private static ByteString handleToByteString(long tableId, Long k) {
    if (k != null) {
      return RowKey.toRowKey(tableId, new IntHandle(k)).toByteString();
    }
    return ByteString.EMPTY;
  }

  private static KeyRange keyRangeByHandle(long tableId, Long s, Long e) {
    return keyRangeByHandle(tableId, s, Status.EQUAL, e, Status.EQUAL);
  }

  private static TiRegion region(long id, KeyRange range) {
    return new TiRegion(
        Metapb.Region.newBuilder()
            .setId(id)
            .setStartKey(encodeKey(range.getStart().toByteArray()))
            .setEndKey(encodeKey(range.getEnd().toByteArray()))
            .addPeers(Peer.getDefaultInstance())
            .build(),
        null,
        IsolationLevel.RC,
        CommandPri.Low);
  }

  @Test
  public void splitRangeByRegionTest() {
    MockRegionManager mgr =
        new MockRegionManager(
            ImmutableList.of(keyRange(null, 30L), keyRange(30L, 50L), keyRange(50L, null)));
    RangeSplitter s = RangeSplitter.newSplitter(mgr);
    List<RangeSplitter.RegionTask> tasks =
        s.splitRangeByRegion(
            ImmutableList.of(
                keyRange(0L, 40L), keyRange(41L, 42L), keyRange(45L, 50L), keyRange(70L, 1000L)));

    assertEquals(tasks.get(0).getRegion().getId(), 0);
    assertEquals(tasks.get(0).getRanges().size(), 1);
    assertEquals(tasks.get(0).getRanges().get(0), keyRange(0L, 30L));

    assertEquals(tasks.get(1).getRegion().getId(), 1);
    assertEquals(tasks.get(1).getRanges().get(0), keyRange(30L, 40L));
    assertEquals(tasks.get(1).getRanges().get(1), keyRange(41L, 42L));
    assertEquals(tasks.get(1).getRanges().get(2), keyRange(45L, 50L));
    assertEquals(tasks.get(1).getRanges().size(), 3);

    assertEquals(tasks.get(2).getRegion().getId(), 2);
    assertEquals(tasks.get(2).getRanges().size(), 1);
    assertEquals(tasks.get(2).getRanges().get(0), keyRange(70L, 1000L));
  }

  @Test
  public void splitAndSortHandlesByRegionTest() {
    final long tableId = 1;
    List<Handle> handles = new ArrayList<>();
    handles.add(new IntHandle(1));
    handles.add(new IntHandle(5));
    handles.add(new IntHandle(4));
    handles.add(new IntHandle(3));
    handles.add(new IntHandle(10));
    handles.add(new IntHandle(2));
    handles.add(new IntHandle(100));
    handles.add(new IntHandle(101));
    handles.add(new IntHandle(99));
    handles.add(new IntHandle(88));
    handles.add(new IntHandle(-1));
    handles.add(new IntHandle(-255));
    handles.add(new IntHandle(-100));
    handles.add(new IntHandle(-99));
    handles.add(new IntHandle(-98));
    handles.add(new IntHandle(Long.MIN_VALUE));
    handles.add(new IntHandle(8960));
    handles.add(new IntHandle(8959));
    handles.add(new IntHandle(19999));
    handles.add(new IntHandle(15001));

    MockRegionManager mgr =
        new MockRegionManager(
            ImmutableList.of(
                keyRangeByHandle(tableId, null, Status.EQUAL, -100L, Status.EQUAL),
                keyRangeByHandle(tableId, -100L, Status.EQUAL, 10L, Status.GREATER),
                keyRangeByHandle(tableId, 10L, Status.GREATER, 50L, Status.EQUAL),
                keyRangeByHandle(tableId, 50L, Status.EQUAL, 100L, Status.GREATER),
                keyRangeByHandle(tableId, 100L, Status.GREATER, 9000L, Status.LESS),
                keyRangeByHandle(tableId, 0x2300L /*8960*/, Status.LESS, 16000L, Status.EQUAL),
                keyRangeByHandle(tableId, 16000L, Status.EQUAL, null, Status.EQUAL)));

    RangeSplitter s = RangeSplitter.newSplitter(mgr);
    List<RangeSplitter.RegionTask> tasks =
        new ArrayList<>(s.splitAndSortHandlesByRegion(ImmutableList.of(tableId), handles));
    tasks.sort(
        (l, r) -> {
          Long regionIdLeft = l.getRegion().getId();
          Long regionIdRight = r.getRegion().getId();
          return regionIdLeft.compareTo(regionIdRight);
        });

    // [-INF, -100): [Long.MIN_VALUE, Long.MIN_VALUE + 1), [-255, -254)
    assertEquals(tasks.get(0).getRegion().getId(), 0);
    assertEquals(tasks.get(0).getRanges().size(), 2);
    assertEquals(
        tasks.get(0).getRanges().get(0),
        keyRangeByHandle(tableId, Long.MIN_VALUE, Long.MIN_VALUE + 1));
    assertEquals(tasks.get(0).getRanges().get(1), keyRangeByHandle(tableId, -255L, -254L));

    // [-100, 10.x): [-100, -97), [-1, 0), [1, 6), [10, 11)
    assertEquals(tasks.get(1).getRegion().getId(), 1);
    assertEquals(tasks.get(1).getRanges().size(), 4);
    assertEquals(tasks.get(1).getRanges().get(0), keyRangeByHandle(tableId, -100L, -97L));
    assertEquals(tasks.get(1).getRanges().get(1), keyRangeByHandle(tableId, -1L, 0L));
    assertEquals(tasks.get(1).getRanges().get(2), keyRangeByHandle(tableId, 1L, 6L));
    assertEquals(tasks.get(1).getRanges().get(3), keyRangeByHandle(tableId, 10L, 11L));

    // [10.x, 50): empty
    // [50, 100.x): [88, 89) [99, 101)
    assertEquals(tasks.get(2).getRegion().getId(), 3);
    assertEquals(tasks.get(2).getRanges().size(), 2);
    assertEquals(tasks.get(2).getRanges().get(0), keyRangeByHandle(tableId, 88L, 89L));
    assertEquals(tasks.get(2).getRanges().get(1), keyRangeByHandle(tableId, 99L, 101L));

    // [100.x, less than 8960): [101, 102) [8959, 8960)
    assertEquals(tasks.get(3).getRegion().getId(), 4);
    assertEquals(tasks.get(3).getRanges().size(), 2);
    assertEquals(tasks.get(3).getRanges().get(0), keyRangeByHandle(tableId, 101L, 102L));
    assertEquals(tasks.get(3).getRanges().get(1), keyRangeByHandle(tableId, 8959L, 8960L));

    // [less than 8960, 16000): [9000, 9001), [15001, 15002)
    assertEquals(tasks.get(4).getRegion().getId(), 5);
    assertEquals(tasks.get(4).getRanges().size(), 2);
    assertEquals(tasks.get(4).getRanges().get(0), keyRangeByHandle(tableId, 8960L, 8961L));
    assertEquals(tasks.get(4).getRanges().get(1), keyRangeByHandle(tableId, 15001L, 15002L));

    // [16000, INF): [19999, 20000)
    assertEquals(tasks.get(5).getRegion().getId(), 6);
    assertEquals(tasks.get(5).getRanges().size(), 1);
    assertEquals(tasks.get(5).getRanges().get(0), keyRangeByHandle(tableId, 19999L, 20000L));
  }

  @Test
  public void groupByAndSortHandlesByRegionIdTest() {
    final long tableId = 1;
    List<Handle> handles = new ArrayList<>();
    handles.add(new IntHandle(1));
    handles.add(new IntHandle(5));
    handles.add(new IntHandle(4));
    handles.add(new IntHandle(3));
    handles.add(new IntHandle(10));
    handles.add(new IntHandle(11));
    handles.add(new IntHandle(12));
    handles.add(new IntHandle(2));
    handles.add(new IntHandle(100));
    handles.add(new IntHandle(101));
    handles.add(new IntHandle(99));
    handles.add(new IntHandle(88));
    handles.add(new IntHandle(-1));
    handles.add(new IntHandle(-255));
    handles.add(new IntHandle(-100));
    handles.add(new IntHandle(-99));
    handles.add(new IntHandle(-98));
    handles.add(new IntHandle(Long.MIN_VALUE));
    handles.add(new IntHandle(8960));
    handles.add(new IntHandle(8959));
    handles.add(new IntHandle(19999));
    handles.add(new IntHandle(15001));
    handles.add(new IntHandle(99999999999L));
    handles.add(new IntHandle(Long.MAX_VALUE));

    MockRegionManager mgr =
        new MockRegionManager(
            ImmutableList.of(
                keyRangeByHandle(tableId, null, Status.EQUAL, -100L, Status.EQUAL),
                keyRangeByHandle(tableId, -100L, Status.EQUAL, 10L, Status.GREATER),
                keyRangeByHandle(tableId, 10L, Status.GREATER, 50L, Status.EQUAL),
                keyRangeByHandle(tableId, 50L, Status.EQUAL, 100L, Status.GREATER),
                keyRangeByHandle(tableId, 100L, Status.GREATER, 9000L, Status.LESS),
                keyRangeByHandle(tableId, 0x2300L /*8960*/, Status.LESS, 16000L, Status.EQUAL),
                keyRangeByHandle(tableId, 16000L, Status.EQUAL, null, Status.EQUAL)));

    TLongObjectHashMap<List<Handle>> result = new TLongObjectHashMap<>();
    RangeSplitter.newSplitter(mgr)
        .groupByAndSortHandlesByRegionId(tableId, handles)
        .forEach((k, v) -> result.put(k.first.getId(), v));
    assertEquals(2, result.get(0).size());
    assertEquals(10, result.get(1).size());
    assertEquals(2, result.get(2).size());
    assertEquals(3, result.get(3).size());
    assertEquals(2, result.get(4).size());
    assertEquals(2, result.get(5).size());
    assertEquals(3, result.get(6).size());
  }

  static class MockRegionManager extends RegionManager {
    private final Map<KeyRange, TiRegion> mockRegionMap;

    MockRegionManager(List<KeyRange> ranges) {
      super(null, null);
      mockRegionMap =
          ranges.stream().collect(Collectors.toMap(kr -> kr, kr -> region(ranges.indexOf(kr), kr)));
    }

    @Override
    public TiRegion getRegionById(long regionId) {
      return mockRegionMap
          .entrySet()
          .stream()
          .filter(e -> e.getValue().getId() == regionId)
          .findFirst()
          .get()
          .getValue();
    }

    @Override
    public Pair<TiRegion, Metapb.Store> getRegionStorePairByKey(
        ByteString key, TiStoreType storeType, BackOffer backOffer) {
      for (Map.Entry<KeyRange, TiRegion> entry : mockRegionMap.entrySet()) {
        KeyRange range = entry.getKey();
        if (KeyRangeUtils.makeRange(range.getStart(), range.getEnd()).contains(Key.toRawKey(key))) {
          TiRegion region = entry.getValue();
          return Pair.create(region, Metapb.Store.newBuilder().setId(region.getId()).build());
        }
      }
      return null;
    }
  }
}
