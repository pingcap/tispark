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
import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;

import java.util.ArrayList;
import java.util.List;

public class KeyRangeUtils {
  public static Range toRange(Coprocessor.KeyRange range) {
    if (range == null || (range.getStart().isEmpty() && range.getEnd().isEmpty())) {
      return Range.all();
    }
    if (range.getStart().isEmpty()) {
      return Range.lessThan(Comparables.wrap(range.getEnd()));
    }
    if (range.getEnd().isEmpty()) {
      return Range.atLeast(Comparables.wrap(range.getStart()));
    }
    return Range.closedOpen(Comparables.wrap(range.getStart()), Comparables.wrap(range.getEnd()));
  }

  public static List<Coprocessor.KeyRange> split(Coprocessor.KeyRange range, int splitFactor) {
    if (splitFactor > 32 || splitFactor <= 0 || (splitFactor & (splitFactor - 1)) != 0) {
      throw new TiClientInternalException(
          "splitFactor must be positive integer power of 2 and no greater than 16");
    }

    ByteString startKey = range.getStart();
    ByteString endKey = range.getEnd();
    // we don't cut infinite
    if (startKey.isEmpty() || endKey.isEmpty()) {
      return ImmutableList.of(range);
    }

    ImmutableList.Builder<Coprocessor.KeyRange> resultList = ImmutableList.builder();
    int maxSize = Math.max(startKey.size(), endKey.size());
    int i;

    for (i = 0; i < maxSize; i++) {
      byte sb = i < startKey.size() ? startKey.byteAt(i) : 0;
      byte eb = i < endKey.size() ? endKey.byteAt(i) : 0;
      if (sb != eb) {
        break;
      }
    }

    ByteString sRemaining = i < startKey.size() ? startKey.substring(i) : ByteString.EMPTY;
    ByteString eRemaining = i < endKey.size() ? endKey.substring(i) : ByteString.EMPTY;

    CodecDataInput cdi = new CodecDataInput(sRemaining);
    int uss = cdi.readPartialUnsignedShort();

    cdi = new CodecDataInput(eRemaining);
    int ues = cdi.readPartialUnsignedShort();

    int delta = (ues - uss) / splitFactor;
    if (delta <= 0) {
      return ImmutableList.of(range);
    }

    ByteString prefix = startKey.size() > endKey.size() ?
                        startKey.substring(0, i) : endKey.substring(0, i);
    ByteString newStartKey = startKey;
    ByteString newEndKey;
    for (int j = 0; j < splitFactor; j++) {
      uss += delta;
      if (j == splitFactor - 1) {
        newEndKey = endKey;
      } else {
        CodecDataOutput cdo = new CodecDataOutput();
        cdo.writeShort(uss);
        newEndKey = prefix.concat(cdo.toByteString());
      }
      resultList.add(makeCoprocRange(newStartKey, newEndKey));
      newStartKey = newEndKey;
    }

    return resultList.build();
  }

  public static String toString(Coprocessor.KeyRange range) {
    return String.format("Start:[%s], End: [%s]",
        TableCodec.decodeRowKey(range.getStart()),
        TableCodec.decodeRowKey(range.getEnd()));
  }

  public static String toString(Coprocessor.KeyRange range, List<DataType> types) {
    if (range == null || types == null) {
      return "";
    }
    try {
      return String.format("{[%s], [%s]}",
          TableCodec.decodeIndexSeekKeyToString(range.getStart(), types),
          TableCodec.decodeIndexSeekKeyToString(range.getEnd(), types));
    } catch (Exception ignore) {}
    return range.toString();
  }

  public static List<DataType> getIndexColumnTypes(TiTableInfo table, TiIndexInfo index) {
    ImmutableList.Builder<DataType> types = ImmutableList.builder();
    for (TiIndexColumn indexColumn : index.getIndexColumns()) {
      TiColumnInfo tableColumn = table.getColumns().get(indexColumn.getOffset());
      types.add(tableColumn.getType());
    }
    return types.build();
  }

  public static Range makeRange(ByteString startKey, ByteString endKey) {
    if (startKey.isEmpty() && endKey.isEmpty()) {
      return Range.all();
    }
    if (startKey.isEmpty()) {
      return Range.lessThan(Comparables.wrap(endKey));
    } else if (endKey.isEmpty()) {
      return Range.atLeast(Comparables.wrap(startKey));
    }
    return Range.closedOpen(Comparables.wrap(startKey), Comparables.wrap(endKey));
  }

  @SuppressWarnings("unchecked")
  public static List<KeyRange> mergeRanges(List<KeyRange> ranges) {
    if (ranges == null || ranges.isEmpty() || ranges.size() == 1) {
      return ranges;
    }

    Range merged = ranges
        .stream()
        .map(KeyRangeUtils::toRange)
        .reduce(Range::span).get();

    List<KeyRange> keyRanges = new ArrayList<>();
    try {
      Comparables.ComparableByteString lower =
          (Comparables.ComparableByteString) merged.lowerEndpoint();
      Comparables.ComparableByteString upper =
          (Comparables.ComparableByteString) merged.upperEndpoint();

      keyRanges.add(makeCoprocRange(lower.getByteString(), upper.getByteString()));
    } catch (Exception ignored) {}

    return keyRanges;
  }

  static Coprocessor.KeyRange makeCoprocRange(ByteString startKey, ByteString endKey) {
    return KeyRange.newBuilder().setStart(startKey).setEnd(endKey).build();
  }

  static Coprocessor.KeyRange makeCoprocRangeWithHandle(long tableId, long startHandle, long endHandle) {
    ByteString startKey = TableCodec.encodeRowKeyWithHandle(tableId, startHandle);
    ByteString endKey = TableCodec.encodeRowKeyWithHandle(tableId, endHandle);
    return KeyRange.newBuilder().setStart(startKey).setEnd(endKey).build();
  }

  static String formatByteString(ByteString key) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < key.size(); i++) {
      sb.append(key.byteAt(i) & 0xff);
      if (i < key.size() - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }
}