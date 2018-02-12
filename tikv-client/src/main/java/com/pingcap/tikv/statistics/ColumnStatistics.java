package com.pingcap.tikv.statistics;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.predicates.IndexRange;

import java.util.List;

public class ColumnStatistics {
  private Histogram histogram;
  private CMSketch cmSketch;
  private long count;
  private TiColumnInfo columnInfo;

  public ColumnStatistics(Histogram histogram, CMSketch cmSketch, long count, TiColumnInfo columnInfo) {
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

  /**
   * getColumnRowCount estimates the row count by a slice of ColumnRange.
   */
  public double getColumnRowCount(List<IndexRange> columnRanges) {
    double rowCount = 0.0;
    for (IndexRange ir : columnRanges) {
      double cnt = 0.0;
      Key pointKey = ir.hasAccessKey() ? ir.getAccessKey() : Key.EMPTY;
      Range<TypedKey> range = ir.getRange();
      Key lPointKey;
      Key uPointKey;

      Key lKey;
      Key uKey;
      if (!ir.hasRange()) {
//        lPointKey = pointKey;
//        uPointKey = pointKey.next();
        cnt = histogram.equalRowCount(pointKey);
      } else {
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
        Key lowerBound = Key.toRawKey(cdo.toByteString());
        cdo.reset();
        cdo.write(uPointKey.getBytes());
        cdo.write(uKey.getBytes());
        Key upperBound = Key.toRawKey(cdo.toByteString());
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
