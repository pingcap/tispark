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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.codec;

import com.pingcap.tikv.exception.CodecException;
import com.pingcap.tikv.key.CommonHandle;
import com.pingcap.tikv.key.Handle;
import com.pingcap.tikv.key.IntHandle;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.Row;
import java.util.Arrays;
import java.util.List;

public class TableCodec {


  // IndexVersionFlag is the flag used to decode the index's version info.
  public static byte IndexVersionFlag = 125;
  // PartitionIDFlag is the flag used to decode the partition ID in global index value.
  public static byte PartitionIDFlag = 126;
  // CommonHandleFlag is the flag used to decode the common handle in an unique index value.
  public static byte CommonHandleFlag = 127;
  // RestoreDataFlag is the flag that RestoreData begin with.
  // See rowcodec.Encoder.Encode and rowcodec.row.toBytes
  public static byte RestoreDataFlag = (byte) RowV2.CODEC_VER;

  public static class IndexValueSegments {

    byte[] commonHandle;
    byte[] partitionID;
    byte[] restoredValues;
    byte[] intHandle;
  }

  public static byte[] encodeRow(
      List<TiColumnInfo> columnInfos,
      Object[] values,
      boolean isPkHandle,
      boolean encodeWithNewRowFormat)
      throws IllegalAccessException {
    if (columnInfos.size() != values.length) {
      throw new IllegalAccessException(
          String.format(
              "encodeRow error: data and columnID count not " + "match %d vs %d",
              columnInfos.size(), values.length));
    }
    if (encodeWithNewRowFormat) {
      return TableCodecV2.encodeRow(columnInfos, values, isPkHandle);
    }
    return TableCodecV1.encodeRow(columnInfos, values, isPkHandle);
  }

  public static Row decodeRow(byte[] value, Handle handle, TiTableInfo tableInfo) {
    if (value.length == 0) {
      throw new CodecException("Decode fails: value length is zero");
    }
    if ((value[0] & 0xff) == RowV2.CODEC_VER) {
      return TableCodecV2.decodeRow(value, handle, tableInfo);
    }
    return TableCodecV1.decodeRow(value, handle, tableInfo);
  }

  public static Handle decodeHandle(byte[] value, boolean isCommonHandle) {
    if (isCommonHandle) {
      return new CommonHandle(value);
    }
    return new IntHandle(new CodecDataInput(value).readLong());
  }

  /* only for unique index */
  public static Handle decodeHandleInUniqueIndexValue(
      byte[] value, boolean isCommonHandle) {
    if (!isCommonHandle) {
      return new IntHandle(new CodecDataInput(value).readLong());
    }
    CodecDataInput codecDataInput = new CodecDataInput(value);
    if (getIndexVersion(value) == 1) {
      IndexValueSegments segments = splitIndexValueForClusteredIndexVersion1(codecDataInput);
      return new CommonHandle(segments.commonHandle);
    }
    int handleLen = ((int) value[2]) << 8 + value[3];
    byte[] encode = Arrays.copyOfRange(value, 4, handleLen + 4);
    return new CommonHandle(encode);
  }

  /* only for unique index */
  public static byte[] genIndexValueForClusteredIndexVersion1(TiIndexInfo index, Handle handle,
      boolean distinct) {
    CodecDataOutput cdo = new CodecDataOutput();
    cdo.writeByte(0);
    cdo.writeByte(IndexVersionFlag);
    cdo.writeByte(1);

    assert (index.isUnique());
    if (distinct) {
      encodeCommonHandle(cdo, handle);
    }
    return cdo.toBytes();
  }

  private static void encodeCommonHandle(CodecDataOutput cdo, Handle handle) {
    cdo.write(CommonHandleFlag);
    byte[] encoded = handle.encoded();
    int hLen = encoded.length;
    cdo.writeShort(hLen);
    cdo.write(encoded);
  }

  private static int getIndexVersion(byte[] value) {
    int tailLen = value[0];
    if ((tailLen == 0 || tailLen == 1) && value[1] == IndexVersionFlag) {
      return value[2];
    }
    return 0;
  }

  //  public static IndexValueSegments splitIndexValueForClusteredIndexVersion1(byte[] value) {
//    int tailLen = value[0];
//    IndexValueSegments segments = new IndexValueSegments();
//    value = Arrays.copyOfRange(value, 3, value.length - tailLen);
//    if (value.length > 0 && value[0] == CommonHandleFlag) {
//      int handleLen = (((int) value[1]) << 8) + value[2];
//      segments.commonHandle = Arrays.copyOfRange(value, 3, 3 + handleLen);
//      value = Arrays.copyOfRange(value, handleLen + 3, value.length);
//    }
//    if (value.length > 0 && value[0] == PartitionIDFlag) {
//      segments.partitionID = Arrays.copyOfRange(value, 1, 9);
//      value = Arrays.copyOfRange(value, 9, value.length);
//    }
//    if (value.length > 0 && value[0] == RestoreDataFlag) {
//      segments.restoredValues = value;
//    }
//    return segments;
//  }
  public static IndexValueSegments splitIndexValueForClusteredIndexVersion1(
      CodecDataInput codecDataInput) {
    int tailLen = codecDataInput.readByte();
    // read IndexVersionFlag
    codecDataInput.readByte();
    // read IndexVersion
    codecDataInput.readByte();
    IndexValueSegments segments = new IndexValueSegments();
    if (codecDataInput.available() > 0 && codecDataInput.peekByte() == CommonHandleFlag) {
      codecDataInput.readByte();
      int handleLen = codecDataInput.readShort();
      segments.commonHandle=new byte[handleLen];
      codecDataInput.readFully(segments.commonHandle, 0, handleLen);
    }
    if (codecDataInput.available() > 0 && codecDataInput.peekByte() == PartitionIDFlag) {
      codecDataInput.readByte();
      segments.partitionID=new byte[9];
      codecDataInput.readFully(segments.partitionID, 0, 9);
    }
    if (codecDataInput.available() > 0 && codecDataInput.peekByte() == RestoreDataFlag) {
      codecDataInput.readByte();
      segments.restoredValues=new byte[codecDataInput.available()-tailLen];
      codecDataInput.readFully(segments.restoredValues, 0, codecDataInput.available() - tailLen);
    }
    return segments;
  }
}
