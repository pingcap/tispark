/*
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

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.predicates.IndexRange;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.MySQLType;
import java.util.List;

/**
 * Each Index will have a single {@link IndexStatistics} to store {@link Histogram} info and {@link
 * CMSketch} info, if any.
 */
public class IndexStatistics {
  private Histogram histogram;
  private CMSketch cmSketch;
  private TiIndexInfo indexInfo;

  public IndexStatistics(Histogram histogram, CMSketch cmSketch, TiIndexInfo indexInfo) {
    this.histogram = histogram;
    this.cmSketch = cmSketch;
    this.indexInfo = indexInfo;
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

  public TiIndexInfo getIndexInfo() {
    return indexInfo;
  }

  public void setIndexInfo(TiIndexInfo indexInfo) {
    this.indexInfo = indexInfo;
  }

  public double getRowCount(List<IndexRange> indexRanges) {
    double rowCount = 0.0;
    for (IndexRange ir : indexRanges) {
      double cnt = 0.0;
      Key pointKey = ir.hasAccessKey() ? ir.getAccessKey() : Key.EMPTY;
      Range<TypedKey> range = ir.getRange();
      Key lPointKey;
      Key uPointKey;

      Key lKey;
      Key uKey;
      if (pointKey != Key.EMPTY) {
        Key convertedKey =
            TypedKey.toTypedKey(pointKey.getBytes(), DataTypeFactory.of(MySQLType.TypeBlob));
        Key convertedNext =
            TypedKey.toTypedKey(
                pointKey.nextPrefix().getBytes(), DataTypeFactory.of(MySQLType.TypeBlob));
        // TODO: Implement CMSketch point query
        //        if (cmSketch != null) {
        //          rowCount += cmSketch.queryBytes(convertedKey.getBytes());
        //        } else {
        rowCount += histogram.betweenRowCount(convertedKey, convertedNext);
        //        }
      }
      if (range != null) {
        lPointKey = pointKey;
        uPointKey = pointKey;

        if (!range.hasLowerBound()) {
          // -INF
          lKey = Key.MIN;
        } else {
          lKey = range.lowerEndpoint();
          if (range.lowerBoundType().equals(BoundType.OPEN)) {
            lKey = lKey.next();
          }
        }
        if (!range.hasUpperBound()) {
          // INF
          uKey = Key.MAX;
        } else {
          uKey = range.upperEndpoint();
          if (range.upperBoundType().equals(BoundType.CLOSED)) {
            uKey = uKey.next();
          }
        }
        CodecDataOutput cdo = new CodecDataOutput();
        cdo.write(lPointKey.getBytes());
        cdo.write(lKey.getBytes());
        Key lowerBound = TypedKey.toTypedKey(cdo.toBytes(), DataTypeFactory.of(MySQLType.TypeBlob));
        cdo.reset();
        cdo.write(uPointKey.getBytes());
        cdo.write(uKey.getBytes());
        Key upperBound = TypedKey.toTypedKey(cdo.toBytes(), DataTypeFactory.of(MySQLType.TypeBlob));
        cnt += histogram.betweenRowCount(lowerBound, upperBound);
      }
      rowCount += cnt;
    }
    if (rowCount > histogram.totalRowCount()) {
      rowCount = histogram.totalRowCount();
    } else if (rowCount < 0) {
      rowCount = 0;
    }
    return rowCount;
  }
}
