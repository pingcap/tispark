package com.pingcap.tikv.codec;

import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.ObjectRowImpl;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType.EncodeType;
import com.pingcap.tikv.types.IntegerType;
import java.util.HashMap;
import java.util.List;

public class TableCodec {
  /**
   * Row layout: colID1, value1, colID2, value2, .....
   *
   * @param columnInfos
   * @param values
   * @return
   * @throws IllegalAccessException
   */
  public static byte[] encodeRow(
      List<TiColumnInfo> columnInfos, Object[] values, boolean isPkHandle)
      throws IllegalAccessException {
    if (columnInfos.size() != values.length) {
      throw new IllegalAccessException(
          String.format(
              "encodeRow error: data and columnID count not " + "match %d vs %d",
              columnInfos.size(), values.length));
    }

    CodecDataOutput cdo = new CodecDataOutput();

    for (int i = 0; i < columnInfos.size(); i++) {
      TiColumnInfo col = columnInfos.get(i);
      if (!col.canSkip(isPkHandle)) {
        IntegerCodec.writeLongFully(cdo, col.getId(), false);
        col.getType().encode(cdo, EncodeType.VALUE, values[i]);
      }
    }

    // We could not set nil value into kv.
    if (cdo.toBytes().length == 0) {
      return new byte[] {Codec.NULL_FLAG};
    }

    return cdo.toBytes();
  }

  public static Row decodeRow(byte[] value, Long handle, TiTableInfo tableInfo) {
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
    Object[] res = new Object[tableInfo.getColumns().size()];
    while (!cdi.eof()) {
      long colID = (long) IntegerType.BIGINT.decode(cdi);
      Object colValue = idToColumn.get(colID).getType().decode(cdi);
      decodedDataMap.put(colID, colValue);
    }

    // construct Row with Map<ColumnID, Data> & handle
    for (int i = 0; i < tableInfo.getColumns().size(); i++) {
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

  public static long decodeHandle(byte[] value) {
    return new CodecDataInput(value).readLong();
  }
}
