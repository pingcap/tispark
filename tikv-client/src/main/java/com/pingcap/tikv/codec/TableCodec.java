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

package com.pingcap.tikv.codec;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.TableCodec.DecodeResult.Status;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.util.FastByteComparisons;
import com.pingcap.tikv.util.Pair;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;

// Basically all protobuf ByteString involves buffer copy
// and might not be a good choice inside codec so we choose to
// return byte directly for future manipulation
// But this is not quite clean in case talking with GRPC interfaces
public class TableCodec {
  public static final int ID_LEN = 8;
  public static final int PREFIX_LEN = 1 + ID_LEN;
  public static final int RECORD_ROW_KEY_LEN = PREFIX_LEN + ID_LEN;

  public static final byte[] TBL_PREFIX = new byte[] {'t'};
  public static final byte[] REC_PREFIX_SEP = new byte[] {'_', 'r'};
  public static final byte[] IDX_PREFIX_SEP = new byte[] {'_', 'i'};

  public static final long SIGN_MASK = ~Long.MAX_VALUE;

  public static void writeRowKey(CodecDataOutput cdo, long tableId, byte[] encodeHandle) {
    Objects.requireNonNull(cdo, "cdo cannot be null");
    appendTableRecordPrefix(cdo, tableId);
    cdo.write(encodeHandle);
  }

  // EncodeIndexSeekKey encodes an index value to kv.Key.
  public static void writeIndexSeekKey(
      CodecDataOutput cdo, long tableId, long indexId, byte[]... dataGroup) {
    Objects.requireNonNull(cdo, "cdo cannot be null");
    appendTableIndexPrefix(cdo, tableId);
    IntegerType.writeLong(cdo, indexId);
    for (byte[] data : dataGroup) {
      if (data != null) {
        cdo.write(data);
      }
    }
  }

  public static String decodeIndexSeekKeyToString(ByteString indexKey, List<DataType> types) {
    Objects.requireNonNull(indexKey, "indexKey cannot be null");
    CodecDataInput cdi = new CodecDataInput(indexKey);
    cdi.skipBytes(TBL_PREFIX.length);
    IntegerType.readLong(cdi);
    cdi.skipBytes(IDX_PREFIX_SEP.length);
    IntegerType.readLong(cdi);

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < types.size(); i++) {
      DataType type = types.get(i);
      // Here is a hack since range is always on last position
      // If last flag is min flag without any other bytes,
      // then we don't try decoding as bytes type
      int remaining = cdi.available();
      if (remaining == 0) break;

      if (i != 0) {
        sb.append(", ");
      }
      int flag = cdi.peekByte();
      if (remaining == 1 && flag == DataType.indexMinValueFlag()) {
        sb.append("-INF");
        cdi.skipBytes(1);
      } else if (remaining == 1 && flag == DataType.indexMaxValueFlag()) {
        sb.append("+INF");
        cdi.skipBytes(1);
      } else {
        Object v = type.decode(cdi);
        if (v == null) {
          sb.append("NULL");
        } else {
          if (v instanceof Date) {
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
            sb.append(fmt.format((Date)v));
          } else {
            sb.append(v.toString());
          }
        }
      }
    }

    return sb.toString();
  }

  // appendTableRecordPrefix appends table record prefix  "t[tableID]_r".
  // tablecodec.go:appendTableRecordPrefix
  private static void appendTableRecordPrefix(CodecDataOutput cdo, long tableId) {
    Objects.requireNonNull(cdo, "cdo cannot be null");
    cdo.write(TBL_PREFIX);
    IntegerType.writeLong(cdo, tableId);
    cdo.write(REC_PREFIX_SEP);
  }

  // appendTableIndexPrefix appends table index prefix  "t[tableID]_i".
  // tablecodec.go:appendTableIndexPrefix
  private static void appendTableIndexPrefix(CodecDataOutput cdo, long tableId) {
    Objects.requireNonNull(cdo, "cdo cannot be null");
    cdo.write(TBL_PREFIX);
    IntegerType.writeLong(cdo, tableId);
    cdo.write(IDX_PREFIX_SEP);
  }

  // encodeRowKeyWithHandle encodes the table id, row handle into a bytes buffer/array
  public static ByteString encodeRowKeyWithHandle(long tableId, long handle) {
    return ByteString.copyFrom(encodeRowKeyWithHandleBytes(tableId, handle));
  }

  // encodeRowKeyWithHandle encodes the table id, row handle into a bytes buffer/array
  public static byte [] encodeRowKeyWithHandleBytes(long tableId, long handle) {
    CodecDataOutput cdo = new CodecDataOutput();
    writeRowKeyWithHandle(cdo, tableId, handle);
    return cdo.toBytes();
  }

  public static long decodeRowKey(byte[] rowKey) {
    Objects.requireNonNull(rowKey, "rowKey cannot be null");
    CodecDataInput cdi = new CodecDataInput(rowKey);
    cdi.skipBytes(TBL_PREFIX.length);
    IntegerType.readLong(cdi);
    cdi.skipBytes(REC_PREFIX_SEP.length);

    return IntegerType.readLong(cdi);
  }

  public static class DecodeResult {
    public long handle;
    public enum Status {
      MIN,
      MAX,
      EQUAL,
      LESS,
      GREATER,
      UNKNOWN_INF
    }
    public Status status;
  }

  public static void tryDecodeRowKey(long tableId, byte[] rowKey, DecodeResult outResult) {
    Objects.requireNonNull(rowKey, "rowKey cannot be null");
    if (rowKey.length == 0) {
      outResult.status = Status.UNKNOWN_INF;
      return;
    }
    CodecDataOutput cdo = new CodecDataOutput();
    appendTableRecordPrefix(cdo, tableId);
    byte [] tablePrefix = cdo.toBytes();

    int res = FastByteComparisons.compareTo(
        tablePrefix, 0, tablePrefix.length,
        rowKey, 0, Math.min(rowKey.length, tablePrefix.length));

    if (res > 0) {
      outResult.status = Status.MIN;
      return;
    }
    if (res < 0) {
      outResult.status = Status.MAX;
      return;
    }

    CodecDataInput cdi = new CodecDataInput(rowKey);
    cdi.skipBytes(tablePrefix.length);
    if (cdi.available() == 8) {
      outResult.status = Status.EQUAL;
    } else if (cdi.available() < 8) {
      outResult.status = Status.LESS;
    } else if (cdi.available() > 8) {
      outResult.status = Status.GREATER;
    }
    outResult.handle = IntegerType.readPartialLong(cdi);
  }

  public static long decodeRowKey(ByteString rowKey) {
    return decodeRowKey(rowKey.toByteArray());
  }

  public static void writeRowKeyWithHandle(CodecDataOutput cdo, long tableId, long handle) {
    Objects.requireNonNull(cdo, "cdo cannot be null");
    appendTableRecordPrefix(cdo, tableId);
    IntegerType.writeLong(cdo, handle);
  }

  public static void writeRecordKey(CodecDataOutput cdo, byte[] recordPrefix, long handle) {
    Objects.requireNonNull(cdo, "cdo cannot be null");
    cdo.write(recordPrefix);
    IntegerType.writeLong(cdo, handle);
  }

  public static Pair<Long, Long> readRecordKey(CodecDataInput cdi) {
    Objects.requireNonNull(cdi, "cdi cannot be null");
    if (!consumeAndMatching(cdi, TBL_PREFIX)) {
      throw new CodecException("Invalid Table Prefix");
    }
    long tableId = IntegerType.readLong(cdi);

    if (!consumeAndMatching(cdi, REC_PREFIX_SEP)) {
      throw new CodecException("Invalid Record Prefix");
    }

    long handle = IntegerType.readLong(cdi);
    return Pair.create(tableId, handle);
  }

  public static long flipSignBit(long v) {
    return v ^ SIGN_MASK;
  }

  private static boolean consumeAndMatching(CodecDataInput cdi, byte[] prefix) {
    Objects.requireNonNull(cdi, "cdi cannot be null");
    for (byte b : prefix) {
      if (cdi.readByte() != b) {
        return false;
      }
    }
    return true;
  }
}
