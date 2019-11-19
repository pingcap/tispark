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

import static java.util.Objects.requireNonNull;

import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.DAGRequest;
import com.pingcap.tidb.tipb.EncodeType;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.columnar.TiChunk;
import com.pingcap.tikv.columnar.TiChunkColumnVector;
import com.pingcap.tikv.columnar.TiRowColumnVector;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.row.RowReader;
import com.pingcap.tikv.row.RowReaderFactory;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;
import java.util.Iterator;
import java.util.List;

public abstract class CoprocessorIterator<T> implements Iterator<T> {
  protected final TiSession session;
  protected final List<RegionTask> regionTasks;
  protected final DAGRequest dagRequest;
  protected final DataType[] handleTypes;
  //  protected final ExecutorCompletionService<Iterator<SelectResponse>> completionService;
  protected RowReader rowReader;
  protected CodecDataInput dataInput;
  protected boolean eof = false;
  protected int taskIndex;
  protected int chunkIndex;
  protected List<Chunk> chunkList;
  protected SchemaInfer schemaInfer;

  CoprocessorIterator(
      DAGRequest req, List<RegionTask> regionTasks, TiSession session, SchemaInfer infer) {
    this.dagRequest = req;
    this.session = session;
    this.regionTasks = regionTasks;
    this.schemaInfer = infer;
    this.handleTypes = infer.getTypes().toArray(new DataType[] {});
  }

  abstract void submitTasks();

  /**
   * Build a DAGIterator from TiDAGRequest and region tasks to get rows
   *
   * <p>When we are preforming a scan request using coveringIndex, {@link
   * com.pingcap.tidb.tipb.IndexScan} should be used to read index rows. In other circumstances,
   * {@link com.pingcap.tidb.tipb.TableScan} is used to scan table rows.
   *
   * @param req TiDAGRequest built
   * @param regionTasks a list or RegionTask each contains a task on a single region
   * @param session TiSession
   * @return a DAGIterator to be processed
   */
  public static CoprocessorIterator<Row> getRowIterator(
      TiDAGRequest req, List<RegionTask> regionTasks, TiSession session) {
    TiDAGRequest dagRequest = req.copy();
    return new DAGIterator<Row>(
        dagRequest.buildTableScan(),
        regionTasks,
        session,
        SchemaInfer.create(dagRequest),
        dagRequest.getPushDownType()) {
      @Override
      public Row next() {
        return rowReader.readRow(schemaInfer.getTypes().toArray(new DataType[0]));
      }
    };
  }

  /**
   * Build a DAGIterator from TiDAGRequest and region tasks to get rows
   *
   * <p>When we are preforming a scan request using coveringIndex, {@link
   * com.pingcap.tidb.tipb.IndexScan} should be used to read index rows. In other circumstances,
   * {@link com.pingcap.tidb.tipb.TableScan} is used to scan table rows.
   *
   * @param req TiDAGRequest built
   * @param regionTasks a list or RegionTask each contains a task on a single region
   * @param session TiSession
   * @return a DAGIterator to be processed
   */
  public static CoprocessorIterator<TiChunk> getTiChunkIterator(
      TiDAGRequest req, List<RegionTask> regionTasks, TiSession session) {
    TiDAGRequest dagRequest = req.copy();
    return new DAGIterator<TiChunk>(
        dagRequest.buildTableScan(),
        regionTasks,
        session,
        SchemaInfer.create(dagRequest),
        dagRequest.getPushDownType()) {
      @Override
      public TiChunk next() {
        // TODO make it configurable
        int numOfRows = 1024;
        DataType[] dataTypes = this.schemaInfer.getTypes().toArray(new DataType[0]);
        // TODO tiColumnarBatch is meant to be reused in the entire data loading process.
        // TODO we need have some fallback solution to handle tikv's response using default encode.
        if (this.encodeType == EncodeType.TypeDefault) {
          Row[] rows = new Row[numOfRows];
          int count = 0;
          for (int i = 0; i < rows.length && hasNext(); i++) {
            rows[i] = rowReader.readRow(dataTypes);
            count += 1;
          }
          TiRowColumnVector[] columnarVectors = new TiRowColumnVector[dataTypes.length];
          for (int i = 0; i < dataTypes.length; i++) {
            columnarVectors[i] = new TiRowColumnVector(dataTypes[i], i, rows, count);
          }
          return new TiChunk(columnarVectors);
        } else {
          // hasNext => create dataInput, so we do not need to advance next dataInput.
          TiChunkColumnVector[] columnarVectors = new TiChunkColumnVector[dataTypes.length];
          for (int i = 0; i < dataTypes.length; i++) {
            // TODO when data return in TypeChunk format, one chunk means one column.
            columnarVectors[i] = dataTypes[i].decodeColumn(dataInput);
          }
          // left data should be trashed.
          dataInput = new CodecDataInput(new byte[0]);

          return new TiChunk(columnarVectors);
        }
      }
    };
  }

  /**
   * Build a DAGIterator from TiDAGRequest and region tasks to get handles
   *
   * <p>When we use getHandleIterator, we must be preforming a IndexScan.
   *
   * @param req TiDAGRequest built
   * @param regionTasks a list or RegionTask each contains a task on a single region
   * @param session TiSession
   * @return a DAGIterator to be processed
   */
  public static CoprocessorIterator<Long> getHandleIterator(
      TiDAGRequest req, List<RegionTask> regionTasks, TiSession session) {
    return new DAGIterator<Long>(
        req.buildIndexScan(),
        regionTasks,
        session,
        SchemaInfer.create(req, true),
        req.getPushDownType()) {
      @Override
      public Long next() {
        return rowReader.readRow(handleTypes).getLong(handleTypes.length - 1);
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
    if (0 > chunkIndex || chunkIndex >= chunkList.size()) {
      throw new IllegalArgumentException();
    }
    dataInput = new CodecDataInput(chunkList.get(chunkIndex).getRowsData());
    rowReader = RowReaderFactory.createRowReader(dataInput);
  }
}
