/*
 * Copyright 2019 PingCAP, Inc.
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

package com.pingcap.tikv.columnar;

/**
 * TiChunk is an abstraction of Chunk data transmitted from TiKV. A Chunk represents a batch row
 * data in columnar format.
 */
public class TiChunk {
  private final TiColumnVector[] columnVectors;
  private final int numOfRows;

  public TiChunk(TiColumnVector[] columnVectors) {
    this.columnVectors = columnVectors;
    this.numOfRows = columnVectors[0].numOfRows();
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
