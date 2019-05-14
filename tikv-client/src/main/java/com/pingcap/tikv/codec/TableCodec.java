package com.pingcap.tikv.codec;

import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.row.DefaultRowReader;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.row.RowReader;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataType.EncodeType;
import com.pingcap.tikv.types.IntegerType;
import gnu.trove.list.array.TLongArrayList;
import java.util.ArrayList;
import java.util.List;

public class TableCodec {
  /**
   * Row layout: colID1, value1, colID2, value2, .....
   *
   * @param columnInfos
   * @param colIDs
   * @param values
   * @return
   * @throws IllegalAccessException
   */
  public static byte[] encodeRow(
      List<TiColumnInfo> columnInfos, TLongArrayList colIDs, Object[] values, boolean isPkHandle)
      throws IllegalAccessException {
    if (columnInfos.size() != colIDs.size()) {
      throw new IllegalAccessException(
          String.format(
              "encodeRow error: data and columnID count not " + "match %d vs %d",
              columnInfos.size(), colIDs.size()));
    }

    CodecDataOutput cdo = new CodecDataOutput();

    for (int i = 0; i < columnInfos.size(); i++) {
      TiColumnInfo col = columnInfos.get(i);
      if (!col.canSkip(isPkHandle)) {
        IntegerCodec.writeLongFully(cdo, colIDs.get(i), false);
        col.getType().encode(cdo, EncodeType.VALUE, values[i]);
      }
    }

    // We could not set nil value into kv.
    if (values.length == 0) {
      return new byte[] {Codec.NULL_FLAG};
    }

    return cdo.toBytes();
  }

  public static Object[] decodeRow(CodecDataInput cdi, List<TiColumnInfo> cols) {
    List<DataType> newColTypes = new ArrayList<>();
    for (TiColumnInfo col : cols) {
      newColTypes.add(IntegerType.BIGINT);
      newColTypes.add(col.getType());
    }
    RowReader rowReader = DefaultRowReader.create(cdi);
    Row row = rowReader.readRow(newColTypes.toArray(new DataType[0]));
    Object[] res = new Object[2 * cols.size()];
    for (int i = 0; i < cols.size(); i++) {
      res[2 * i] = row.get(2 * i, IntegerType.BIGINT);
      res[2 * i + 1] = row.get(2 * i + 1, cols.get(i).getType());
    }
    return res;
  }
}
