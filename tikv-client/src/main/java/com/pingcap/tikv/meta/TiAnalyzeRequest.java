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

package com.pingcap.tikv.meta;

import com.pingcap.tidb.tipb.*;
import com.pingcap.tikv.kvproto.Coprocessor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class TiAnalyzeRequest implements Serializable {
  private static final int maxSampleSize        = 10000;
  private static final int maxRegionSampleSize  = 1000;
  private static final int maxSketchSize        = 10000;
  private static final int maxBucketSize        = 256;
  private static final int defaultCMSketchDepth = 5;
  private static final int defaultCMSketchWidth = 2048;
  private final long startTs = Long.MAX_VALUE; // startTs in Analyze request usually defaults to Long.MAX_VALUE
  private final List<Coprocessor.KeyRange> ranges = new ArrayList<>();
  private TiIndexInfo indexInfo;
  private Collection<ColumnInfo> columnInfos = new ArrayList<>();

  public TiIndexInfo getIndexInfo() {
    return indexInfo;
  }

  public void setIndexInfo(TiIndexInfo indexInfo) {
    this.indexInfo = indexInfo;
  }

  public void addColumnInfos(Collection<ColumnInfo> columnInfos) {
    this.columnInfos.addAll(columnInfos);
  }

  public void addRange(Coprocessor.KeyRange range) {
    ranges.add(range);
  }

  public void addRanges(Collection<Coprocessor.KeyRange> ranges) {
    this.ranges.addAll(ranges);
  }

  public List<Coprocessor.KeyRange> getRanges() {
    return ranges;
  }

  public AnalyzeReq buildIndexAnalyzeReq() {
    requireNonNull(indexInfo, "IndexInfo cannot be null");
    AnalyzeIndexReq req = AnalyzeIndexReq.newBuilder()
        .setBucketSize(maxBucketSize)
        .setCmsketchDepth(defaultCMSketchDepth)
        .setCmsketchWidth(defaultCMSketchWidth)
        .setNumColumns(indexInfo.getIndexColumns().size())
        .build();

    return AnalyzeReq.newBuilder()
        .setStartTs(startTs)
        .setTp(AnalyzeType.TypeIndex)
        .setIdxReq(req)
        .build();
  }

  public AnalyzeReq buildColumnAnalyzeReq() {
    AnalyzeColumnsReq req = AnalyzeColumnsReq.newBuilder()
        .setBucketSize(maxBucketSize)
        .addAllColumnsInfo(columnInfos)
        .setSampleSize(maxSampleSize)
        .setCmsketchDepth(defaultCMSketchDepth)
        .setCmsketchWidth(defaultCMSketchWidth)
        .setSampleSize(maxRegionSampleSize)
        .setSketchSize(maxSketchSize)
        .build();

    return AnalyzeReq.newBuilder()
        .setStartTs(startTs)
        .setTp(AnalyzeType.TypeColumn)
        .setColReq(req)
        .build();
  }
}
