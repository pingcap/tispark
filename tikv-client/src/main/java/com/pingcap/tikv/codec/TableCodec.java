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
  public static byte[] encodeRow(DataType[] colTypes, TLongArrayList colIDs, Object[] values)
      throws IllegalAccessException {
    if (colTypes.length != colIDs.size()) {
      throw new IllegalAccessException(
          String.format(
              "encodeRow error: data and columnID count not " + "match %d vs %d",
              colTypes.length, colIDs.size()));
    }

    // Row layout: colID1, value1, colID2, value2, .....
    CodecDataOutput cdo = new CodecDataOutput();

    for (int i = 0; i < colTypes.length; i++) {
      IntegerCodec.writeLongFully(cdo, colIDs.get(i), false);
      colTypes[i].encode(cdo, EncodeType.VALUE, values[i]);
    }

    // We could not set nil value into kv.
    if (values.length == 0) {
      return new byte[] {Codec.NULL_FLAG};
    }

    byte[] res = cdo.toBytes();
    cdo.reset();
    return res;
  }

  public static Object[] decodeRow(CodecDataInput cdi, DataType[] colTypes) {
    List<DataType> newColTypes = new ArrayList<>();
    for (DataType row1 : colTypes) {
      newColTypes.add(IntegerType.BIGINT);
      newColTypes.add(row1);
    }
    RowReader rowReader = DefaultRowReader.create(cdi);
    Row row = rowReader.readRow(newColTypes.toArray(new DataType[0]));
    Object[] res = new Object[2 * colTypes.length];
    for (int i = 0; i < colTypes.length; i++) {
      res[2 * i] = row.get(2 * i, IntegerType.BIGINT);
      res[2 * i + 1] = row.get(2 * i + 1, colTypes[i]);
    }
    return res;
  }
}
