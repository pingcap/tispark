package com.pingcap.tikv.statistics;

import com.pingcap.tikv.meta.TiIndexInfo;

/**
 * Created by birdstorm on 2017/8/14.
 *
 */
public class IndexWithHistogram {
  private Histogram hg;
  private TiIndexInfo info;

  public IndexWithHistogram(Histogram hist, TiIndexInfo indexInfo) {
    this.hg = hist;
    this.info = indexInfo;
  }

  long getLastUpdateVersion() {
    return hg.getLastUpdateVersion();
  }

//  double getRowCount(List<IndexRange> indexRanges, long tableID) {
//    double rowCount = 0.0;
//    for (IndexRange range : indexRanges) {
//      double cnt = 0.0;
//      Key accessKey = range.getAccessKey();
//      if (accessKey != null) {
//        cnt += hg.equalRowCount(accessKey);
//      }
//      if (range.getRange() != null){
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
////        System.out.println("=>Index " + l + (!lNull ? Key.create(lower) : "-∞")
////            + "," + (!rNull ? Key.create(upper) : "∞") + r);
//        t = DataTypeFactory.of(MySQLType.TypeLong);
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

  public TiIndexInfo getIndexInfo() {
    return info;
  }
}
