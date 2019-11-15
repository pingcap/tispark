package com.pingcap.tikv.columnar;

public class TiColumnarChunk {
  private TiColumnVector[] columnVectors;
  private int numOfRows;

  public TiColumnarChunk(TiColumnVector[] columnVectors) {
    this.columnVectors = columnVectors;
  }

  public TiColumnVector column(int ordinal) {
    return columnVectors[ordinal];
  }

  public int numOfCols() {
    return columnVectors.length;
  }

  public int numOfRows() {
    return numOfRows;
  }
}
