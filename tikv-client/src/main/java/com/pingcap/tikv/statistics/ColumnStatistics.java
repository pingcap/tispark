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

import com.pingcap.tikv.meta.TiColumnInfo;

/**
 * Each Column will have a single {@link ColumnStatistics} to store {@link Histogram} info and
 * {@link CMSketch} info, if any.
 */
public class ColumnStatistics {
  private Histogram histogram;
  private CMSketch cmSketch;
  private long count;
  private TiColumnInfo columnInfo;

  public ColumnStatistics(
      Histogram histogram, CMSketch cmSketch, long count, TiColumnInfo columnInfo) {
    this.histogram = histogram;
    this.cmSketch = cmSketch;
    this.count = count;
    this.columnInfo = columnInfo;
  }

  public Histogram getHistogram() {
    return histogram;
  }

  public void setHistogram(Histogram histogram) {
    this.histogram = histogram;
  }

  public CMSketch getCmSketch() {
    return cmSketch;
  }

  public void setCmSketch(CMSketch cmSketch) {
    this.cmSketch = cmSketch;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public TiColumnInfo getColumnInfo() {
    return columnInfo;
  }

  public void setColumnInfo(TiColumnInfo columnInfo) {
    this.columnInfo = columnInfo;
  }
}
