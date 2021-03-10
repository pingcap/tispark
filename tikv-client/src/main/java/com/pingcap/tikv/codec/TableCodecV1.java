/*
 * Copyright 2020 PingCAP, Inc.
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

package com.pingcap.tikv.codec;

import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.key.Handle;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.ObjectRowImpl;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType.EncodeType;
import com.pingcap.tikv.types.IntegerType;
import java.util.HashMap;
import java.util.List;

public class TableCodecV1 {
  /** Row layout: colID1, value1, colID2, value2, ..... */
  protected static byte[] encodeRow(
      List<TiColumnInfo> columnInfos, Object[] values, boolean isPkHandle) {
    CodecDataOutput cdo = new CodecDataOutput();

    for (int i = 0; i < columnInfos.size(); i++) {
      TiColumnInfo col = columnInfos.get(i);
      // skip pk is handle case
      if (col.isPrimaryKey() && isPkHandle) {
        continue;
      }
      IntegerCodec.writeLongFully(cdo, col.getId(), false);
      col.getType().encode(cdo, EncodeType.VALUE, values[i]);
    }

    // We could not set nil value into kv.
    if (cdo.toBytes().length == 0) {
      return new byte[] {Codec.NULL_FLAG};
    }

    return cdo.toBytes();
  }

  protected static Row decodeRow(byte[] value, Handle handle, TiTableInfo tableInfo) {
    if (handle == null && tableInfo.isPkHandle()) {
      throw new IllegalArgumentException("when pk is handle, handle cannot be null");
    }

    int colSize = tableInfo.getColumns().size();
    HashMap<Long, TiColumnInfo> idToColumn = new HashMap<>(colSize);
    for (TiColumnInfo col : tableInfo.getColumns()) {
      idToColumn.put(col.getId(), col);
    }

    // decode bytes to Map<ColumnID, Data>
    HashMap<Long, Object> decodedDataMap = new HashMap<>(colSize);
    CodecDataInput cdi = new CodecDataInput(value);
    Object[] res = new Object[colSize];
    while (!cdi.eof()) {
      long colID = (long) IntegerType.BIGINT.decode(cdi);
      Object colValue = idToColumn.get(colID).getType().decodeForBatchWrite(cdi);
      decodedDataMap.put(colID, colValue);
    }

    // construct Row with Map<ColumnID, Data> & handle
    for (int i = 0; i < colSize; i++) {
      // skip pk is handle case
      TiColumnInfo col = tableInfo.getColumn(i);
      if (col.isPrimaryKey() && tableInfo.isPkHandle()) {
        res[i] = handle;
      } else {
        res[i] = decodedDataMap.get(col.getId());
      }
    }

    return ObjectRowImpl.create(res);
  }
}
