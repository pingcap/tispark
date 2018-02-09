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
 *
 */

package com.pingcap.tikv.statistics;

import com.pingcap.tikv.key.Key;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Histogram {

  //Histogram
  private final long id;
  private final long numberOfDistinctValue; // Number of distinct values.
  private List<Bucket> buckets;
  private final long nullCount;
  private final long lastUpdateVersion;

  public static class Builder {
    private long id;
    private long NDV;
    private List<Bucket> buckets = new ArrayList<>();
    private long nullCount;
    private long lastUpdateVersion;

    public Builder setId(long id) {
      this.id = id;
      return this;
    }

    public Builder setNDV(long NDV) {
      this.NDV = NDV;
      return this;
    }

    public Builder setBuckets(List<Bucket> buckets) {
      this.buckets = buckets;
      return this;
    }

    public Builder setNullCount(long nullCount) {
      this.nullCount = nullCount;
      return this;
    }

    public Builder setLastUpdateVersion(long lastUpdateVersion) {
      this.lastUpdateVersion = lastUpdateVersion;
      return this;
    }

    public Histogram build() {
      return new Histogram(id, NDV, buckets, nullCount, lastUpdateVersion);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private Histogram(long id,
                    long numberOfDistinctValue,
                    List<Bucket> buckets,
                    long nullCount,
                    long lastUpdateVersion) {
    this.id = id;
    this.numberOfDistinctValue = numberOfDistinctValue;
    this.buckets = buckets;
    this.nullCount = nullCount;
    this.lastUpdateVersion = lastUpdateVersion;
  }

  public long getNumberOfDistinctValue() {
    return numberOfDistinctValue;
  }

  public List<Bucket> getBuckets() {
    return buckets;
  }

  public long getNullCount() {
    return nullCount;
  }

  long getLastUpdateVersion() {
    return lastUpdateVersion;
  }

  public long getId() {
    return id;
  }


  /**
   * equalRowCount estimates the row count where the column equals to value.
   */
  double equalRowCount(Key values) {
    int index = lowerBound(values);
    //index not in range
    if (index == -buckets.size() - 1) {
      return 0;
    }
    // index found
    if (index >= 0) {
      return buckets.get(index).getRepeats();
    }
    //index not found
    index = -index - 1;
    int cmp;
    if (buckets.get(index).getLowerBound() == null) {
      cmp = 1;
    } else {
      Objects.requireNonNull(buckets.get(index).getLowerBound());
      cmp = values.compareTo(buckets.get(index).getLowerBound());
    }
    if (cmp < 0) {
      return 0;
    }
    return totalRowCount() / numberOfDistinctValue;
  }

  /**
   * greaterRowCount estimates the row count where the column greater than value.
   */
  double greaterRowCount(Key values) {
    double lessCount = lessRowCount(values);
    double equalCount = equalRowCount(values);
    double greaterCount;
    greaterCount = totalRowCount() - lessCount - equalCount;
    if (greaterCount < 0) {
      greaterCount = 0;
    }
    return greaterCount;
  }

  /**
   * greaterAndEqRowCount estimates the row count where the column less than or equal to value.
   */
  private double greaterAndEqRowCount(Key values) {
    double greaterCount = greaterRowCount(values);
    double equalCount = equalRowCount(values);
    return greaterCount + equalCount;
  }

  /**
   * lessRowCount estimates the row count where the column less than value.
   */
  double lessRowCount(Key values) {
    int index = lowerBound(values);
    //index not in range
    if (index == -buckets.size() - 1) {
      return totalRowCount();
    }
    if (index < 0) {
      index = -index - 1;
    }
    double curCount = buckets.get(index).count;
    double preCount = 0;
    if (index > 0) {
      preCount = buckets.get(index - 1).count;
    }
    double lessThanBucketValueCount = curCount - buckets.get(index).getRepeats();
    Key lowerBound = buckets.get(index).getLowerBound();
    int c;
    if (lowerBound != null) {
      c = values.compareTo(lowerBound);
    } else {
      c = 1;
    }
    if (c <= 0) {
      return preCount;
    }
    return (preCount + lessThanBucketValueCount) / 2;
  }

  /**
   * lessAndEqRowCount estimates the row count where the column less than or equal to value.
   */
  public double lessAndEqRowCount(Key values) {
    double lessCount = lessRowCount(values);
    double equalCount = equalRowCount(values);
    return lessCount + equalCount;
  }

  /**
   * betweenRowCount estimates the row count where column greater than or equal to a and less than b.
   */
  double betweenRowCount(Key a, Key b) {
//    CodecDataInput c = new CodecDataInput(a.getByteString());
//    CodecDataInput d = new CodecDataInput(b.getByteString());
//    System.out.println(c.readLine() + " with " + d.readLine());
    double lessCountA = lessRowCount(a);
    double lessCountB = lessRowCount(b);
    if (lessCountA >= lessCountB) {
      return inBucketBetweenCount();
    }
    return lessCountB - lessCountA;
  }

  public double totalRowCount() {
    if (buckets.isEmpty()) {
      return 0;
    }
    return (buckets.get(buckets.size() - 1).count);
  }

  public double bucketRowCount() {
    return totalRowCount() / buckets.size();
  }

  public double inBucketBetweenCount() {
    // TODO: Make this estimation more accurate using uniform spread assumption.
    return bucketRowCount() / 3 + 1;
  }

  /**
   * lowerBound returns the smallest index of the searched key
   * and returns (-[insertion point] - 1) if the key is not found in buckets
   * where [insertion point] denotes the index of the first element greater than the key
   */
  public int lowerBound(Key key) {
    assert key.getClass() == buckets.get(0).getUpperBound().getClass();
    return Arrays.binarySearch(buckets.toArray(), new Bucket(key));
  }

  /**
   * mergeBuckets is used to merge every two neighbor buckets.
   *
   * @param bucketIdx: index of the last bucket.
   */
  public void mergeBlock(int bucketIdx) {
    int curBuck = 0;
    for (int i = 0; i + 1 <= bucketIdx; i += 2) {
      buckets.set(curBuck++, new Bucket(buckets.get(i + 1).count,
          buckets.get(i + 1).getRepeats(),
          buckets.get(i + 1).getLowerBound(),
          buckets.get(i).getUpperBound()));
    }
    if (bucketIdx % 2 == 0) {
      buckets.set(curBuck++, buckets.get(bucketIdx));
    }
    buckets = buckets.subList(0, curBuck);
  }

  /**
   * getIncreaseFactor will return a factor of data increasing after the last analysis.
   */
  double getIncreaseFactor(long totalCount) {
    long columnCount = buckets.get(buckets.size() - 1).count + nullCount;
    if (columnCount == 0) {
      return 1.0;
    }
    return (double) totalCount / (double) columnCount;
  }
}
