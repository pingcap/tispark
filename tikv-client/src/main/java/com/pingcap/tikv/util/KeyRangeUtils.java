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

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.key.Key;
import java.util.List;
import org.tikv.kvproto.Coprocessor.KeyRange;

public class KeyRangeUtils {
  public static Range<Key> makeRange(ByteString startKey, ByteString endKey) {
    return Range.closedOpen(toRawKey(startKey, true), toRawKey(endKey));
  }

  /**
   * Build a Coprocessor Range with CLOSED_OPEN endpoints
   *
   * @param startKey startKey
   * @param endKey endKey
   * @return a CLOSED_OPEN range for coprocessor
   */
  public static KeyRange makeCoprocRange(ByteString startKey, ByteString endKey) {
    return KeyRange.newBuilder().setStart(startKey).setEnd(endKey).build();
  }

  /**
   * Build a Coprocessor Range
   *
   * @param range Range with Comparable endpoints
   * @return a CLOSED_OPEN range for coprocessor
   */
  public static KeyRange makeCoprocRange(Range<Key> range) {
    if (!range.hasLowerBound() || !range.hasUpperBound()) {
      throw new TiClientInternalException("range is not bounded");
    }
    if (range.lowerBoundType().equals(BoundType.OPEN)
        || range.upperBoundType().equals(BoundType.CLOSED)) {
      throw new TiClientInternalException("range must be CLOSED_OPEN");
    }
    return makeCoprocRange(
        range.lowerEndpoint().toByteString(), range.upperEndpoint().toByteString());
  }

  /**
   * Merge potential discrete ranges into one large range.
   *
   * @param ranges the range list to merge
   * @return the minimal range which encloses all ranges in this range list.
   */
  public static List<KeyRange> mergeRanges(List<KeyRange> ranges) {
    if (ranges == null || ranges.isEmpty() || ranges.size() == 1) {
      return ranges;
    }

    KeyRange first = ranges.get(0);
    Key lowMin = toRawKey(first.getStart(), true);
    Key upperMax = toRawKey(first.getEnd(), false);

    for (int i = 1; i < ranges.size(); i++) {
      KeyRange keyRange = ranges.get(i);
      Key start = toRawKey(keyRange.getStart(), true);
      Key end = toRawKey(keyRange.getEnd(), false);
      if (start.compareTo(lowMin) < 0) {
        lowMin = start;
      }
      if (end.compareTo(upperMax) > 0) {
        upperMax = end;
      }
    }

    ImmutableList.Builder<KeyRange> rangeBuilder = ImmutableList.builder();
    rangeBuilder.add(makeCoprocRange(lowMin.toByteString(), upperMax.toByteString()));
    return rangeBuilder.build();
  }

  /**
   * Merge SORTED potential discrete ranges into one large range.
   *
   * @param ranges the sorted range list to merge
   * @return the minimal range which encloses all ranges in this range list.
   */
  public static List<KeyRange> mergeSortedRanges(List<KeyRange> ranges) {
    return mergeSortedRanges(ranges, 1);
  }

  /**
   * Merge SORTED potential discrete ranges into no more than {@code splitNum} large range.
   *
   * @param ranges the sorted range list to merge
   * @param splitNum upper bound of number of ranges to merge into
   * @return the minimal range which encloses all ranges in this range list.
   */
  private static List<KeyRange> mergeSortedRanges(List<KeyRange> ranges, int splitNum) {
    if (splitNum <= 0) {
      throw new RuntimeException("Cannot split ranges by non-positive integer");
    }
    if (ranges == null || ranges.isEmpty() || ranges.size() <= splitNum) {
      return ranges;
    }
    // use ceil for split step
    int step = (ranges.size() + splitNum - 1) / splitNum;
    ImmutableList.Builder<KeyRange> rangeBuilder = ImmutableList.builder();
    for (int i = 0, nowPos = 0; i < splitNum; i++) {
      int nextPos = Math.min(nowPos + step - 1, ranges.size() - 1);
      KeyRange first = ranges.get(nowPos);
      KeyRange last = ranges.get(nextPos);

      rangeBuilder.add(makeCoprocRange(first.getStart(), last.getEnd()));
      nowPos = nowPos + step;
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
