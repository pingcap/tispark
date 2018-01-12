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


import static com.pingcap.tikv.key.Key.toRawKey;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import java.util.List;
import java.util.Set;

public class KeyRangeUtils {
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

  public static Range<Key> makeRange(ByteString startKey, ByteString endKey) {
    return Range.closedOpen(toRawKey(startKey, true), toRawKey(endKey));
  }

  public static KeyRange makeCoprocRange(ByteString startKey, ByteString endKey) {
    return KeyRange.newBuilder().setStart(startKey).setEnd(endKey).build();
  }

  public static KeyRange makeCoprocRange(Range<Key> range) {
    if (!range.hasLowerBound() || !range.hasUpperBound()) {
      throw new TiClientInternalException("range is not closed");
    }
    return makeCoprocRange(range.lowerEndpoint().toByteString(),
                           range.upperEndpoint().toByteString());
  }

  public static List<KeyRange> mergeRanges(List<KeyRange> ranges) {
    if (ranges == null || ranges.isEmpty() || ranges.size() == 1) {
      return ranges;
    }

    RangeSet<Key> rangeSet = TreeRangeSet.create();
    for (KeyRange keyRange : ranges) {
      Range<Key> range = makeRange(keyRange.getStart(), keyRange.getEnd());
      rangeSet.add(range);
    }

    Set<Range<Key>> mergedRanges = rangeSet.asRanges();
    ImmutableList.Builder<KeyRange> rangeBuilder = ImmutableList.builder();
    for (Range<Key> range : mergedRanges) {
      rangeBuilder.add(makeCoprocRange(range));
    }

    return rangeBuilder.build();
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