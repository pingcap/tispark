/*
 * Copyright 2017 PingCAP, Inc.
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

package com.pingcap.tikv.operation.iterator;

import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.DAGRequest;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.row.RowReader;
import com.pingcap.tikv.row.RowReaderFactory;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

public abstract class CoprocessIterator<T> implements Iterator<T> {
  protected final TiSession session;
  protected final List<RegionTask> regionTasks;
  protected DAGRequest dagRequest;
  protected static final DataType[] handleTypes = new DataType[]{IntegerType.INT};
  //  protected final ExecutorCompletionService<Iterator<SelectResponse>> completionService;
  protected RowReader rowReader;
  protected CodecDataInput dataInput;
  protected boolean eof = false;
  protected int taskIndex;
  protected int chunkIndex;
  protected List<Chunk> chunkList;
  protected SchemaInfer schemaInfer;

  CoprocessIterator(DAGRequest req,
                    List<RegionTask> regionTasks,
                    TiSession session,
                    SchemaInfer infer) {
    this.dagRequest = req;
    this.session = session;
    this.regionTasks = regionTasks;
    this.schemaInfer = infer;
  }

  abstract void submitTasks();

  public static CoprocessIterator<Row> getRowIterator(TiDAGRequest req,
                                                      List<RegionTask> regionTasks,
                                                      TiSession session) {
    return new DAGIterator<Row>(
        req.buildScan(req.isIndexScan()),
        regionTasks,
        session,
        SchemaInfer.create(req),
        req.getPushDownType()
    ) {
      @Override
      public Row next() {
        if (hasNext()) {
          return rowReader.readRow(schemaInfer.getTypes().toArray(new DataType[0]));
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  public static CoprocessIterator<Long> getHandleIterator(TiDAGRequest req,
                                                          List<RegionTask> regionTasks,
                                                          TiSession session) {
    return new DAGIterator<Long>(
        req.buildScan(true),
        regionTasks,
        session,
        SchemaInfer.create(req),
        req.getPushDownType()
    ) {
      @Override
      public Long next() {
        if (hasNext()) {
          return rowReader.readRow(handleTypes).getLong(0);
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  boolean tryAdvanceChunkIndex() {
    if (chunkList == null || chunkIndex >= chunkList.size() - 1) {
      return false;
    }

    chunkIndex++;
    return true;
  }

  void createDataInputReader() {
    requireNonNull(chunkList, "Chunk list should not be null.");
    if (0 > chunkIndex ||
        chunkIndex >= chunkList.size()) {
      throw new IllegalArgumentException();
    }
    dataInput = new CodecDataInput(chunkList.get(chunkIndex).getRowsData());
    rowReader = RowReaderFactory.createRowReader(dataInput);
  }
}
