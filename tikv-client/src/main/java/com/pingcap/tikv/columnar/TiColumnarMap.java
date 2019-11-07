package com.pingcap.tikv.columnar;

import org.apache.spark.sql.catalyst.util.MapData;

/** Map abstraction in {@link TiColumnVector}. */
public final class TiColumnarMap extends MapData {

  private final TiColumnarArray keys;
  private final TiColumnarArray values;
  private final int length;

  public TiColumnarMap(TiColumnVector keys, TiColumnVector values, int offset, int length) {
    this.length = length;
    this.keys = new TiColumnarArray(keys, offset, length);
    this.values = new TiColumnarArray(values, offset, length);
  }

  @Override
  public int numElements() {
    return length;
  }

  @Override
  public TiColumnarArray keyArray() {
    return keys;
  }

  @Override
  public TiColumnarArray valueArray() {
    return values;
  }

  @Override
  public TiColumnarMap copy() {
    throw new UnsupportedOperationException();
  }
}
