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
      if (pointKey!= Key.EMPTY) {
        rowCount += histogram.equalRowCount(TypedKey.toTypedKey(pointKey.getBytes(), DataTypeFactory.of(MySQLType.TypeBlob)));
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
