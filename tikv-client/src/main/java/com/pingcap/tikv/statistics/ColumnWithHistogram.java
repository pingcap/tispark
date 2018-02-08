package com.pingcap.tikv.statistics;

import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.meta.TiColumnInfo;

import java.util.Set;

/**
 * Created by birdstorm on 2017/8/14.
 * may be deleted according to TiDB's implementation
 */
public class ColumnWithHistogram {
  private Histogram hg;
  private TiColumnInfo info;

  public ColumnWithHistogram(Histogram hist, TiColumnInfo colInfo) {
    this.hg = hist;
    this.info = colInfo;
  }

  long getLastUpdateVersion() {
    return hg.getLastUpdateVersion();
  }

  /** getColumnRowCount estimates the row count by a slice of ColumnRange. */
//  double getColumnRowCount(List<IndexRange> columnRanges) {
//    double rowCount = 0.0;
//    for (IndexRange range : columnRanges) {
//      double cnt = 0.0;
//      List<Object> points = range.getAccessPoints();
//      if (!points.isEmpty()) {
//        if (points.size() > 1) {
//          System.out.println("Warning: ColumnRowCount should only contain one attribute.");
//        }
//        cnt = hg.equalRowCount(Key.create(points.get(0)));
//        assert range.getRange() == null;
//      } else if (range.getRange() != null){
//        Range rg = range.getRange();
//        Key lowerBound, upperBound;
//        DataType t;
//        boolean lNull = !rg.hasLowerBound();
//        boolean rNull = !rg.hasUpperBound();
//        boolean lOpen = lNull || rg.lowerBoundType().equals(BoundType.OPEN);
//        boolean rOpen = rNull || rg.upperBoundType().equals(BoundType.OPEN);
//        String l = lOpen ? "(" : "[";
//        String r = rOpen ? ")" : "]";
//        CodecDataOutput cdo = new CodecDataOutput();
//        Object lower = Key.unwrap(!lNull ? rg.lowerEndpoint() : DataType.encodeIndex(cdo));
//        Object upper = Key.unwrap(!rNull ? rg.upperEndpoint() : DataType.encodeMaxValue(cdo));
////        System.out.println("=>Column " + l + (!lNull ? Key.create(lower) : "-∞")
////            + "," + (!rNull ? Key.create(upper) : "∞") + r);
//        t = DataTypeFactory.of(TYPE_LONG);
//        if(lNull) {
//          t.encodeMinValue(cdo);
//        } else {
//          if(lower instanceof Number) {
//            t.encode(cdo, DataType.EncodeType.KEY, lower);
//          } else if(lower instanceof byte[]) {
//            cdo.write(((byte[]) lower));
//          } else {
//            cdo.write(((ByteString) lower).toByteArray());
//          }
//          if(lOpen) {
//            cdo.writeByte(0);
//          }
//        }
//        if(!lNull && lOpen) {
//          lowerBound = Key.create(KeyUtils.prefixNext(cdo.toBytes()));
//        } else {
//          lowerBound = Key.create(cdo.toBytes());
//        }
//
//        cdo.reset();
//        if(rNull) {
//          t.encodeMaxValue(cdo);
//        } else {
//          if(upper instanceof Number) {
//            t.encode(cdo, DataType.EncodeType.KEY, upper);
//          } else if(upper instanceof byte[]) {
//            cdo.write(((byte[]) upper));
//          } else {
//            cdo.write(((ByteString) upper).toByteArray());
//          }
//        }
//        if(!rNull && !rOpen) {
//          upperBound = Key.create(KeyUtils.prefixNext(cdo.toBytes()));
//        } else {
//          upperBound = Key.create(cdo.toBytes());
//        }
//
//
//        cnt += hg.betweenRowCount(lowerBound, upperBound);
//      }
//      rowCount += cnt;
//    }
//    if (rowCount > hg.totalRowCount()) {
//      rowCount = hg.totalRowCount();
//    } else if (rowCount < 0) {
//      rowCount = 0;
//    }
//    return rowCount;
//  }

  public Histogram getHistogram() {
    return hg;
  }

  public TiColumnInfo getColumnInfo() {
    return info;
  }

  public static ColumnRef indexInfo2Col(Set<ColumnRef> cols, TiColumnInfo col) {
    return null;
  }

}
