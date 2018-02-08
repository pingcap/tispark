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

import com.pingcap.tidb.tipb.Histogram;
import com.pingcap.tikv.key.Key;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TiHistogram {
  private final Histogram histogram;
  private final List<Key> upperBounds;
  private final List<Bucket> buckets;

  public TiHistogram(Histogram histogram) {
    this.histogram = histogram;
    this.buckets = new ArrayList<>();
    histogram.getBucketsList().forEach(bucket -> buckets.add(new Bucket(bucket.getCount(), bucket.getRepeats(), bucket.getLowerBound(), bucket.getUpperBound())));
    upperBounds = histogram.getBucketsList().stream().map(bucket -> Key.toRawKey(bucket.getUpperBound())).collect(Collectors.toList());
  }

  public List<Bucket> getBuckets() {
    return buckets;
  }

  private int bucketLen() {
    return buckets.size();
  }

  public long totalRowCount() {
    return buckets.get(bucketLen() - 1).getCount();
  }

  protected int lowerBound(Key value) {
    return Arrays.binarySearch(upperBounds.toArray(), value);
  }

//  public double equalRowCount(Key value) {
//    int index = lowerBound(value);
//
//  }
}
