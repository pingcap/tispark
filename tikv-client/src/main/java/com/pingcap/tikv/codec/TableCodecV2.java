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

import com.pingcap.tikv.key.Handle;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.ObjectRowImpl;
import com.pingcap.tikv.row.Row;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TableCodecV2 {

  /**
   * New Row Format: Reference
   * https://github.com/pingcap/tidb/blob/952d1d7541a8e86be0af58f5b7e3d5e982bab34e/docs/design/2018-07-19-row-format.md
   *
   * <p>- version, flag, numOfNotNullCols, numOfNullCols, notNullCols, nullCols, notNullOffsets,
   * notNullValues
   */
  protected static byte[] encodeRow(
      List<TiColumnInfo> columnInfos, Object[] values, boolean isPkHandle) {
    RowEncoderV2 encoder = new RowEncoderV2();
    List<TiColumnInfo> columnInfoList = new ArrayList<>();
    List<Object> valueList = new ArrayList<>();
    for (int i = 0; i < columnInfos.size(); i++) {
      TiColumnInfo col = columnInfos.get(i);
      // skip pk is handle case
      if (col.isPrimaryKey() && isPkHandle) {
        continue;
      }
      columnInfoList.add(col);
      valueList.add(values[i]);
    }
    return encoder.encode(columnInfoList, valueList);
  }

  protected static Row decodeRow(byte[] value, Handle handle, TiTableInfo tableInfo) {
    if (handle == null && tableInfo.isPkHandle()) {
      throw new IllegalArgumentException("when pk is handle, handle cannot be null");
    }
    int colSize = tableInfo.getColumns().size();
    // decode bytes to Map<ColumnID, Data>
    HashMap<Long, Object> decodedDataMap = new HashMap<>(colSize);
    RowV2 rowV2 = RowV2.createNew(value);

    TiIndexInfo pk = tableInfo.getPrimaryKey();

    if (pk != null) {
      List<TiColumnInfo> cols = new ArrayList<>();
      for (TiIndexColumn indexColumn : pk.getIndexColumns()) {
        TiColumnInfo col = tableInfo.getColumn(indexColumn.getOffset());
        cols.add(col);
      }
      if (tableInfo.isPkHandle()) {
        assert cols.size() == 1;
        decodedDataMap.put(cols.get(0).getId(), handle.data()[0]);
      }
      if (tableInfo.isCommonHandle()) {
        for (int i = 0; i < cols.size(); i++) {
          decodedDataMap.put(cols.get(i).getId(), handle.data()[i]);
        }
      }
    }

    for (TiColumnInfo col : tableInfo.getColumns()) {
      if (decodedDataMap.containsKey(col.getId())) {
        continue;
      } else if (col.isPrimaryKey() && tableInfo.isPkHandle()) {
        decodedDataMap.put(col.getId(), handle);
        continue;
      }
      RowV2.ColIDSearchResult searchResult = rowV2.findColID(col.getId());
      if (searchResult.isNull) {
        // current col is null, nothing should be added to decodedMap
        continue;
      }
      if (!searchResult.notFound) {
        // corresponding column should be found
        assert (searchResult.idx != -1);
        byte[] colData = rowV2.getData(searchResult.idx);
        Object d = RowDecoderV2.decodeCol(colData, col.getType());
        decodedDataMap.put(col.getId(), d);
      }
    }

    Object[] res = new Object[colSize];

    // construct Row with Map<ColumnID, Data> & handle
    for (int i = 0; i < colSize; i++) {
      // skip pk is handle case
      TiColumnInfo col = tableInfo.getColumn(i);
      res[i] = decodedDataMap.get(col.getId());
    }
    return ObjectRowImpl.create(res);
  }
}
