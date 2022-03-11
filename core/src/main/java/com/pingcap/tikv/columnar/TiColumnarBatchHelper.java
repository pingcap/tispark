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

import org.apache.spark.sql.vectorized.ColumnarBatch;

/** A helper class to create {@link ColumnarBatch} from {@link TiChunk} */
public final class TiColumnarBatchHelper {
  public static ColumnarBatch createColumnarBatch(TiChunk chunk) {
    int colLen = chunk.numOfCols();
    TiColumnVectorAdapter[] columns = new TiColumnVectorAdapter[colLen];
    for (int i = 0; i < colLen; i++) {
      columns[i] = new TiColumnVectorAdapter(chunk.column(i));
    }
    ColumnarBatch batch = new ColumnarBatch(columns);
    batch.setNumRows(chunk.numOfRows());
    return batch;
  }
}
