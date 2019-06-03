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

package com.pingcap.tikv.key;

import com.google.common.base.Joiner;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType;
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

  public static IndexKey toIndexKey(long tableId, long indexId, Key... dataKeys) {
    return new IndexKey(tableId, indexId, dataKeys);
  }

  public static Key[] encodeIndexDataValues(
      Row row, List<TiIndexColumn> indexColumns, TiTableInfo tableInfo) {
    Key[] keys = new Key[indexColumns.size()];
    // TODO: truncate column value if necessary.
    for (int i = 0; i < indexColumns.size(); i++) {
      TiIndexColumn col = indexColumns.get(i);
      DataType colTp = tableInfo.getColumn(col.getOffset()).getType();
      Key key = TypedKey.toTypedKey(row.get(col.getOffset(), colTp), colTp);
      keys[i] = key;
    }
    return keys;
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
