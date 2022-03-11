/*
 *
 * Copyright 2018 PingCAP, Inc.
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
 *
 */

package com.pingcap.tikv.statistics;

import com.pingcap.tikv.key.Key;

/**
 * Bucket is histogram element.
 *
 * <p>Bucket bound is the smallest and greatest values stored in the bucket. The lower and upper
 * bound are stored in bucket as lowerBound and upperBound.
 *
 * <p>A bucket count is the number of items stored in all previous buckets and the current bucket.
 * Bucket counts are always in increasing order.
 *
 * <p>A bucket repeat is the number of repeats of the greatest bucket value, it can be used to find
 * popular values.
 *
 * <p>Note that lowerBound and upperBound keys should be 'comparable objects', and these bounds are
 * encoded as `binary` type in TiDB. Intuitively, you should also use Keys encoded as binary format
 * to do comparison in row count estimation.
 */
public class Bucket implements Comparable<Bucket> {
  public long count;
  private long repeats;
  private Key lowerBound;
  private Key upperBound;

  public Bucket(long count, long repeats, Key lowerBound, Key upperBound) {
    this.count = count;
    this.repeats = repeats;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    assert upperBound != null;
  }

  /** used for binary search only */
  public Bucket(Key upperBound) {
    this.upperBound = upperBound;
    assert upperBound != null;
  }

  @Override
  public int compareTo(Bucket b) {
    return upperBound.compareTo(b.upperBound);
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public long getRepeats() {
    return repeats;
  }

  public void setRepeats(long repeats) {
    this.repeats = repeats;
  }

  public Key getLowerBound() {
    return lowerBound;
  }

  public void setLowerBound(Key lowerBound) {
    this.lowerBound = lowerBound;
  }

  public Key getUpperBound() {
    return upperBound;
  }

  public void setUpperBound(Key upperBound) {
    this.upperBound = upperBound;
  }

  @Override
  public String toString() {
    return "{count="
        + count
        + ", repeats="
        + repeats
        + ", range=["
        + lowerBound
        + ", "
        + upperBound
        + "]}";
  }
}
