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
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.row.RowReader;
import com.pingcap.tikv.row.RowReaderFactory;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.TypeSystem;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;
import java.util.Iterator;
import java.util.List;

public abstract class CoprocessIterator<T> implements Iterator<T> {
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

  CoprocessIterator(
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
  public static CoprocessIterator<Row> getRowIterator(
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
   * Build a DAGIterator from TiDAGRequest and region tasks to get handles
   *
   * <p>When we use getHandleIterator, we must be preforming a IndexScan.
   *
   * @param req TiDAGRequest built
   * @param regionTasks a list or RegionTask each contains a task on a single region
   * @param session TiSession
   * @return a DAGIterator to be processed
   */
  public static CoprocessIterator<Long> getHandleIterator(
      TiDAGRequest req, List<RegionTask> regionTasks, TiSession session) {
    return new DAGIterator<Long>(
        req.buildIndexScan(),
        regionTasks,
        session,
        SchemaInfer.create(req, true),
        req.getPushDownType()) {
      @Override
      public Long next() {
        Row row = rowReader.readRow(handleTypes);
        if (TypeSystem.getVersion() == 1) {
          return (long) row.getInteger(handleTypes.length - 1);
        } else {
          return row.getLong(handleTypes.length - 1);
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
    if (0 > chunkIndex || chunkIndex >= chunkList.size()) {
      throw new IllegalArgumentException();
    }
    dataInput = new CodecDataInput(chunkList.get(chunkIndex).getRowsData());
    rowReader = RowReaderFactory.createRowReader(dataInput);
  }
}
