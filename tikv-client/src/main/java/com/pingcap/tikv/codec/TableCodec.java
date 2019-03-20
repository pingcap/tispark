package com.pingcap.tikv.codec;

import com.pingcap.tikv.codec.Codec.IntegerCodec;
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
  // Row layout: colID1, value1, colID2, value2, .....
  public static byte[] encodeRow(
      DataType[] rows, TLongArrayList colIDs, Object[] values, CodecDataOutput cdo)
      throws IllegalAccessException {
    if (rows.length != colIDs.size()) {
      throw new IllegalAccessException(
          String.format(
              "encodeRow error: data and columnID count not " + "match %d vs %d",
              rows.length, colIDs.size()));
    }

    for (int i = 0; i < rows.length; i++) {
      IntegerCodec.writeLongFully(cdo, colIDs.get(i), true);
      rows[i].encode(cdo, EncodeType.VALUE, values[i]);
    }

    // We could not set nil value into kv.
    if (values.length == 0) {
      return new byte[] {Codec.NULL_FLAG};
    }

    return cdo.toBytes();
  }

  public static Object[] decodeRow(CodecDataInput cdi, DataType[] rows) {
    List<DataType> newRows = new ArrayList<>();
    for (DataType row1 : rows) {
      newRows.add(IntegerType.BIGINT);
      newRows.add(row1);
    }
    RowReader rowReader = DefaultRowReader.create(cdi);
    Row row = rowReader.readRow(newRows.toArray(new DataType[0]));
    Object[] res = new Object[2 * rows.length];
    for (int i = 0; i < rows.length; i++) {
      res[2 * i] = row.get(2 * i, IntegerType.BIGINT);
      res[2 * i + 1] = row.get(2 * i + 1, rows[i]);
    }
    return res;
  }
}
