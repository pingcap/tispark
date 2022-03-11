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
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.columnar.BatchedTiChunkColumnVector;
import com.pingcap.tikv.columnar.TiChunk;
import com.pingcap.tikv.columnar.TiChunkColumnVector;
import com.pingcap.tikv.columnar.TiColumnVector;
import com.pingcap.tikv.columnar.TiRowColumnVector;
import com.pingcap.tikv.columnar.datatypes.CHType;
import com.pingcap.tikv.key.CommonHandle;
import com.pingcap.tikv.key.Handle;
import com.pingcap.tikv.key.IntHandle;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.row.RowReader;
import com.pingcap.tikv.row.RowReaderFactory;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.util.CHTypeMapping;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class CoprocessorIterator<T> implements Iterator<T> {
  protected final TiSession session;
  protected final List<RegionTask> regionTasks;
  protected final DAGRequest dagRequest;
  protected final DataType[] handleTypes;
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
    // set encode type to TypeDefault because currently, only
    // CoprocessorIterator<TiChunk> support TypeChunk and TypeCHBlock encode type
    dagRequest.setEncodeType(EncodeType.TypeDefault);
    return new DAGIterator<Row>(
        dagRequest.buildTableScan(),
        regionTasks,
        session,
        SchemaInfer.create(dagRequest),
        dagRequest.getPushDownType(),
        dagRequest.getStoreType(),
        dagRequest.getStartTs().getVersion()) {
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
      TiDAGRequest req, List<RegionTask> regionTasks, TiSession session, int numOfRows) {
    TiDAGRequest dagRequest = req.copy();
    return new DAGIterator<TiChunk>(
        dagRequest.buildTableScan(),
        regionTasks,
        session,
        SchemaInfer.create(dagRequest),
        dagRequest.getPushDownType(),
        dagRequest.getStoreType(),
        dagRequest.getStartTs().getVersion()) {
      @Override
      public TiChunk next() {
        DataType[] dataTypes = this.schemaInfer.getTypes().toArray(new DataType[0]);
        // TODO tiColumnarBatch is meant to be reused in the entire data loading process.
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
        } else if (this.encodeType == EncodeType.TypeChunk) {
          TiColumnVector[] columnarVectors = new TiColumnVector[dataTypes.length];
          List<List<TiChunkColumnVector>> childColumnVectors = new ArrayList<>();
          for (int i = 0; i < dataTypes.length; i++) {
            childColumnVectors.add(new ArrayList<>());
          }

          int count = 0;
          // hasNext will create an dataInput which is our datasource.
          // TODO(Zhexuan Yang) we need control memory limit in case of out of memory error
          while (count < numOfRows && hasNext()) {
            for (int i = 0; i < dataTypes.length; i++) {
              childColumnVectors.get(i).add(dataTypes[i].decodeChunkColumn(dataInput));
            }
            int size = childColumnVectors.get(0).size();
            count += childColumnVectors.get(0).get(size - 1).numOfRows();
            // left data should be trashed.
            dataInput = new CodecDataInput(new byte[0]);
          }

          for (int i = 0; i < dataTypes.length; i++) {
            columnarVectors[i] = new BatchedTiChunkColumnVector(childColumnVectors.get(i), count);
          }

          return new TiChunk(columnarVectors);
        } else {
          // reading column count
          long colCount = IntegerCodec.readUVarLong(dataInput);
          long numOfRows = IntegerCodec.readUVarLong(dataInput);
          TiColumnVector[] columnVectors = new TiColumnVector[(int) colCount];

          for (int columnIdx = 0; columnIdx < colCount; columnIdx++) {
            // reading column name
            long length = IntegerCodec.readUVarLong(dataInput);
            for (int i = 0; i < length; i++) {
              dataInput.readByte();
            }

            // reading type name
            length = IntegerCodec.readUVarLong(dataInput);
            byte[] utf8Bytes = new byte[(int) length];
            for (int i = 0; i < length; i++) {
              utf8Bytes[i] = dataInput.readByte();
            }
            String typeName = new String(utf8Bytes, StandardCharsets.UTF_8);
            CHType type = CHTypeMapping.parseType(typeName);
            columnVectors[columnIdx] = type.decode(dataInput, (int) numOfRows);
            // TODO this is workaround to bybass nullable type
          }
          dataInput = new CodecDataInput(new byte[0]);
          return new TiChunk(columnVectors);
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
  public static CoprocessorIterator<Handle> getHandleIterator(
      TiDAGRequest req, List<RegionTask> regionTasks, TiSession session) {
    TiDAGRequest dagRequest = req.copy();
    // set encode type to TypeDefault because currently, only
    // CoprocessorIterator<TiChunk> support TypeChunk and TypeCHBlock encode type
    dagRequest.setEncodeType(EncodeType.TypeDefault);
    return new DAGIterator<Handle>(
        dagRequest.buildIndexScan(),
        regionTasks,
        session,
        SchemaInfer.create(dagRequest, true),
        dagRequest.getPushDownType(),
        dagRequest.getStoreType(),
        dagRequest.getStartTs().getVersion()) {
      @Override
      public Handle next() {
        Row row = rowReader.readRow(handleTypes);
        Object[] data = new Object[handleTypes.length];
        for (int i = 0; i < handleTypes.length; i++) {
          data[i] = row.get(i, handleTypes[i]);
        }

        if (handleTypes.length == 1 && handleTypes[0] == IntegerType.BIGINT) {
          return new IntHandle((long) data[0]);
        } else {
          return CommonHandle.newCommonHandle(handleTypes, data);
        }
      }
    };
  }

  abstract void submitTasks();

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
