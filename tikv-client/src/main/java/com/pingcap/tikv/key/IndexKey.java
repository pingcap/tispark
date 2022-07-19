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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.key;

import com.google.common.base.Joiner;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import java.util.List;

public class IndexKey extends Key {
  private static final byte[] IDX_PREFIX_SEP = new byte[] {'_', 'i'};

  private final long tableId;
  private final long indexId;
  private final Key[] dataKeys;

  private IndexKey(long tableId, long indexId, Key[] dataKeys) {
    super(encode(tableId, indexId, dataKeys));
    this.tableId = tableId;
    this.indexId = indexId;
    this.dataKeys = dataKeys;
  }

  public static class EncodeIndexDataResult {
    public EncodeIndexDataResult(byte[] indexKey, boolean distinct) {
      this.indexKey = indexKey;
      this.distinct = distinct;
    }

    public byte[] indexKey;
    public boolean distinct;
  }

  public static IndexKey toIndexKey(long tableId, long indexId, Key... dataKeys) {
    return new IndexKey(tableId, indexId, dataKeys);
  }

  public static EncodeIndexDataResult genIndexKey(
      long physicalID, Row row, TiIndexInfo indexInfo, Handle handle, TiTableInfo tableInfo) {
    // When the index is not a unique index,
    // or when the index is a unique index and the index value contains null,
    // set distinct=false and append handle to index key.
    boolean distinct = false;
    List<TiIndexColumn> indexColumns = indexInfo.getIndexColumns();
    if (indexInfo.isUnique()) {
      distinct = true;
      for (TiIndexColumn col : indexColumns) {
        DataType colTp = tableInfo.getColumn(col.getOffset()).getType();
        if (row.get(col.getOffset(), colTp) == null) {
          distinct = false;
          break;
        }
      }
    }
    CodecDataOutput keyCdo = new CodecDataOutput();
    keyCdo.write(IndexKey.toIndexKey(physicalID, indexInfo.getId()).getBytes());
    for (TiIndexColumn col : indexColumns) {
      DataType colTp = tableInfo.getColumn(col.getOffset()).getType();
      // TODO
      // truncate index value when index is prefix index.
      Key key = TypedKey.toTypedKey(row.get(col.getOffset(), colTp), colTp, (int) col.getLength());
      keyCdo.write(key.getBytes());
    }
    if (!distinct) {
      if (handle.isInt()) {
        keyCdo.write(TypedKey.toTypedKey(handle, IntegerType.BIGINT).getBytes());
      } else {
        keyCdo.write(handle.encodedAsKey());
      }
    }
    return new EncodeIndexDataResult(keyCdo.toBytes(), !distinct);
  }

  private static byte[] encode(long tableId, long indexId, Key[] dataKeys) {
    CodecDataOutput cdo = new CodecDataOutput();
    cdo.write(TBL_PREFIX);
    IntegerCodec.writeLong(cdo, tableId);
    cdo.write(IDX_PREFIX_SEP);
    IntegerCodec.writeLong(cdo, indexId);
    for (Key key : dataKeys) {
      if (key == null) {
        throw new TypeException("key cannot be null");
      }
      cdo.write(key.getBytes());
    }
    return cdo.toBytes();
  }

  public long getTableId() {
    return tableId;
  }

  public long getIndexId() {
    return indexId;
  }

  public Key[] getDataKeys() {
    return dataKeys;
  }

  @Override
  public String toString() {
    return String.format("[%s]", Joiner.on(",").useForNull("null").join(dataKeys));
  }
}
