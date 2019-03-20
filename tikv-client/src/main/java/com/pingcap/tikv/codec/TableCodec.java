package com.pingcap.tikv.codec;

import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataType.EncodeType;

public class TableCodec {
  public final long ID_LEN = 8;
  public final long PREFIX_LEN = 1+ID_LEN;
  public final long RECORD_ROW_KEY_LEN= PREFIX_LEN + ID_LEN;
  public final long TBL_PREFIX_LEN=1;
  public final long RECORD_PREFIX_SEP_LEN = 2;
  // the length of key 't{table_id}' which is used for table split.
  public final long TBL_SPLIT_KEY_LEN = 1+ID_LEN;

  protected static final byte[] TBL_PREFIX = new byte[] {'t'};
  protected static final byte[] RECORD_PREFIX_SEP = "_r".getBytes();
  protected static final byte[] INDEX_PREFIX_SEP= "_i".getBytes();


  // Row layout: colID1, value1, colID2, value2, .....
  public static byte[] encodeRow(DataType[] rows, long[] colIDs, Object[] values, CodecDataOutput cdo)
      throws IllegalAccessException {
    if(rows.length != values.length) {
      throw new IllegalAccessException(String.format("EncodeRow error: data and columnID count not "
          + "match %d vs %d", rows.length, colIDs.length));
    }

    for(int i = 0; i < rows.length; i++) {
      cdo.writeLong(colIDs[i]);
      rows[i].encode(cdo, EncodeType.VALUE, values[i]);
    }

    // We could not set nil value into kv.
    if(values.length == 0) {
      return new byte[]{Codec.NULL_FLAG};
    }


    return cdo.toBytes();
  }
}
