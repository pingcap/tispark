package com.pingcap.tikv.statistics;

public class CMSketch {
  private int depth;
  private int width;
  private long count;
  private long[][] table;

  public int getDepth() {
    return depth;
  }

  public void setDepth(int depth) {
    this.depth = depth;
  }

  public int getWidth() {
    return width;
  }

  public void setWidth(int width) {
    this.width = width;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public long[][] getTable() {
    return table;
  }

  public void setTable(long[][] table) {
    this.table = table;
  }
  // Hide constructor
  private CMSketch() {}

  public static CMSketch newCMSketch(int d, int w) {
    CMSketch sketch = new CMSketch();
    sketch.setTable(new long[d][w]);
    sketch.setDepth(d);
    sketch.setWidth(w);
    return sketch;
  }
}
