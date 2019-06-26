package com.pingcap.tikv.codec;

import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.exception.ConvertNotSupportException;
import com.pingcap.tikv.exception.ConvertOverflowException;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.DefaultRowReader;
import com.pingcap.tikv.row.ObjectRowImpl;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.row.RowReader;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataType.EncodeType;
import com.pingcap.tikv.types.IntegerType;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TableCodec {
  /**
   * Row layout: colID1, value1, colID2, value2, .....
   *
   * @param columnInfos
   * @param values
   * @return
   * @throws IllegalAccessException
   * @throws ConvertNotSupportException
   * @throws ConvertOverflowException
   */
  public static byte[] encodeRow(
      List<TiColumnInfo> columnInfos, Object[] values, boolean isPkHandle)
      throws IllegalAccessException, ConvertNotSupportException, ConvertOverflowException {
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
        Object convertedValue = col.getType().convertToTiDBType(values[i]);
        col.getType().encode(cdo, EncodeType.VALUE, convertedValue);
      }
    }

    // We could not set nil value into kv.
    if (cdo.toBytes().length == 0) {
      return new byte[] {Codec.NULL_FLAG};
    }

    return cdo.toBytes();
  }

  public static Row decodeRow(byte[] value, Long handle, TiTableInfo tableInfo) {
    CodecDataInput cdi = new CodecDataInput(value);
    List<DataType> newColTypes = new ArrayList<>();
    List<TiColumnInfo> colsWithoutPK =
        tableInfo
            .getColumns()
            .stream()
            .filter(col -> !col.canSkip(tableInfo.isPkHandle()))
            .collect(Collectors.toList());
    for (TiColumnInfo col : colsWithoutPK) {
      newColTypes.add(IntegerType.BIGINT);
      newColTypes.add(col.getType());
    }

    RowReader rowReader = DefaultRowReader.create(cdi);
    Row row = rowReader.readRow(newColTypes.toArray(new DataType[0]));
    if (handle == null && tableInfo.isPkHandle()) {
      throw new IllegalArgumentException("when pk is handle, handle cannot be null");
    }
    Object[] res = new Object[tableInfo.getColumns().size()];
    int offset = 0;
    for (int i = 0; i < tableInfo.getColumns().size(); i++) {
      // skip pk is handle case
      TiColumnInfo col = tableInfo.getColumn(i);
      if (col.isPrimaryKey() && tableInfo.isPkHandle()) {
        res[i] = handle;
        offset = -1;
      } else {
        res[i] = row.get(2 * (i + offset) + 1, col.getType());
      }
    }

    return ObjectRowImpl.create(res);
  }

  public static long decodeHandle(byte[] value) {
    return new CodecDataInput(value).readLong();
  }
}
