package com.pingcap.tikv.columnar;

/**
 * This class wraps multiple ColumnVectors as a row-wise table. It provides a row view of this batch
 * so that Spark can access the data row by row. Instance of it is meant to be reused during the
 * entire data loading process.
 */
public final class TiColumnarBatch {
  private int numRows;
  private final TiColumnVectorAdapter[] columns;

  /**
   * Called to close all the columns in this batch. It is not valid to access the data after calling
   * this. This must be called at the end to clean up memory allocations.
   */
  public void close() {
    for (TiColumnVectorAdapter c : columns) {
      c.close();
    }
  }

  /** Sets the number of rows in this batch. */
  public void setNumRows(int numRows) {
    this.numRows = numRows;
  }

  /** Returns the number of columns that make up this batch. */
  public int numCols() {
    return columns.length;
  }

  /** Returns the number of rows for read, including filtered rows. */
  public int numRows() {
    return numRows;
  }

  /** Returns the column at `ordinal`. */
  public TiColumnVectorAdapter column(int ordinal) {
    return columns[ordinal];
  }

  public TiColumnarBatch(TiColumnarChunk columnarChunk) {
    this.columns = new TiColumnVectorAdapter[columnarChunk.numOfCols()];
    for(int i = 0; i < columnarChunk.numOfCols(); i++) {
      this.columns[i] = new TiColumnVectorAdapter(columnarChunk.column(i));
    }
    this.numRows = columns[0].numOfRows();
  }
}
