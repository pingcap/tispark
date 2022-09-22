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
import com.pingcap.tikv.meta.Collation;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.BytesType;
import com.pingcap.tikv.types.Converter;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.MySQLType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TableCodec {

  // MaxOldEncodeValueLen is the maximum len of the old encoding of index value.
  public static byte MaxOldEncodeValueLen = 9;
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

  // The encoding code is written to mimic TiDB and removed some logic that we didn't support.
  // The detail encoding explain can be seen here
  // https://github.com/pingcap/tidb/blob/master/tablecodec/tablecodec.go#L1127
  // Value layout:
  //    +-- IndexValueVersion0  (with common handle)
  //		|
  //		|  Layout: TailLen |    Options     | Padding
  // 		|  Length:   1     |  len(options)  | len(padding)
  // 		|
  // 		|  TailLen:       len(padding)
  // 		|  Options:       Encode some value for new features, such as common handle, new collations
  //    |                 or global index.
  // 		|                 See below for more information.
  // 		|  Padding:       Ensure length of value always >= 10. (or >= 11 if UntouchedFlag exists.)
  // 		|
  // 		+-- Old Encoding (integer handle, local)
  // 		|
  // 		|  Layout: [Handle]
  // 		|  Length:   8
  // 		|
  // 		|  Handle:  Only exists in unique index.
  // 		|
  // 		|  If no Handle , value will be one single byte '0' (i.e. []byte{'0'}).
  // 		|  Length of value <= 9, use to distinguish from the new encoding.
  //  	|
  // 		+-- IndexValueForClusteredIndexVersion1
  // 		|
  // 		|  Layout: TailLen |    VersionFlag  |    Version     ｜ Options
  // 		|  Length:   1     |        1        |      1         |  len(options)
  // 		|
  // 		|  TailLen:       TailLen always be zero.
  // 		|  Options:       Encode some value for new features, such as common handle, new collations
  // or global index.
  // 		|                 See below for more information.
  // 		|
  // 		|  Layout of Options:
  // 		|
  //		|     Segment:             Common Handle                 |     Global Index      |   New
  // Collation
  // 		|     Layout:  CHandle flag | CHandle Len | CHandle      | PidFlag | PartitionID |
  // restoreData
  //		|     Length:     1         | 2           | len(CHandle) |    1    |    8        |
  // len(restoreData)
  // 		|
  // 		|     Common Handle Segment: Exists when unique index used common handles.
  //    |     Global Index in not support now.
  //		|     In v4.0, restored data contains all the index values. For example, (a int, b char(10))
  // and index (a, b).
  //		|     In v5.0, restored data contains only non-binary data(except for char and _bin). In the
  // above example, the restored data contains only the value of b.
  //		|     Besides, if the collation of b is _bin, then restored data is an integer indicate the
  // spaces are truncated. Then we use sortKey
  //		|     and the restored data together to restore original data.
  public static byte[] genIndexValue(
      Row row,
      Handle handle,
      int commonHandleVersion,
      boolean distinct,
      TiIndexInfo tiIndexInfo,
      TiTableInfo tiTableInfo) {
    if (!handle.isInt() && commonHandleVersion == 1) {
      return TableCodec.genIndexValueForCommonHandleVersion1(
          row, handle, distinct, tiIndexInfo, tiTableInfo);
    }
    return genIndexValueForClusterIndexVersion0(row, handle, distinct, tiIndexInfo, tiTableInfo);
  }

  // If the following conditions are satisfied, a column should have restore data:
  // The column is a string type that uses the new collation.
  // For version1, type char and blob, the collation is neither binary nor with the suffix “_bin”.
  // For version1, type varchar, the collation is not binary.
  private static void genRestoreData(
      Row row, CodecDataOutput cdo, TiIndexInfo tiIndexInfo, TiTableInfo tiTableInfo) {
    List<TiColumnInfo> columnInfoList = new ArrayList<>();
    List<Object> valueList = new ArrayList<>();
    for (TiIndexInfo index : tiTableInfo.getIndices()) {
      for (TiIndexColumn tiIndexColumn : index.getIndexColumns()) {
        TiColumnInfo indexColumnInfo = tiTableInfo.getColumn(tiIndexColumn.getOffset());
        DataType indexType = indexColumnInfo.getType();
        int prefixLength = (int) tiIndexColumn.getLength();
        if (needRestoreData(indexType)) {
          Object value = row.get(indexColumnInfo.getOffset(), indexColumnInfo.getType());
          if (value == null) {
            continue;
          } else if (Collation.isBinCollation(indexType.getCollationCode())) {
            continue;
          } else if (DataType.isLengthUnSpecified(prefixLength)) {
            valueList.add(value);
          } else if (indexType instanceof BytesType) {
            if (indexType.getCharset().equalsIgnoreCase("utf8")
                || indexType.getCharset().equalsIgnoreCase("utf8mb4")) {
              value = Converter.convertUtf8ToBytes(value, prefixLength);
              valueList.add(value);
            } else {
              value = Converter.convertToBytes(value, prefixLength);
              valueList.add(value);
            }
          }
          columnInfoList.add(indexColumnInfo);
        }
      }
    }
    if (valueList.size() > 0) {
      cdo.write(new RowEncoderV2().encode(columnInfoList, valueList));
    }
  }

  private static byte[] genIndexValueForClusterIndexVersion0(
      Row row, Handle handle, boolean distinct, TiIndexInfo tiIndexInfo, TiTableInfo tiTableInfo) {
    CodecDataOutput cdo = new CodecDataOutput();
    cdo.writeByte(0);
    int tailLen = 0;
    Boolean newEncode = false;
    if (!handle.isInt() && distinct) {
      encodeCommonHandle(cdo, handle);
      newEncode = true;
    }

    // encode restore data if needed.
    // For version0, restore all index value.
    if (tableNeedRestoreData(tiTableInfo, tiIndexInfo)) {
      List<TiColumnInfo> columnInfoList = new ArrayList<>();
      List<Object> valueList = new ArrayList<>();
      for (TiIndexColumn tiIndexColumn : tiIndexInfo.getIndexColumns()) {
        TiColumnInfo indexColumnInfo = tiTableInfo.getColumn(tiIndexColumn.getOffset());
        Object value = row.get(indexColumnInfo.getOffset(), indexColumnInfo.getType());
        valueList.add(value);
        columnInfoList.add(indexColumnInfo);
      }
      if (valueList.size() > 0) {
        cdo.write(new RowEncoderV2().encode(columnInfoList, valueList));
      }
    }

    // when cdo has restore data, we will use newEncode formate.
    if (cdo.size() > 1) {
      newEncode = true;
    }

    if (newEncode) {
      if (handle.isInt() && distinct) {
        tailLen += 8;
        encodeHandleInUniqueIndexValue(cdo, handle);
      } else if (cdo.size() < 10) {
        int paddingLen = 10 - cdo.size();
        tailLen += paddingLen;
        cdo.write(new byte[paddingLen]);
      }
      byte[] value = cdo.toBytes();
      value[0] = (byte) tailLen;
      return value;
    }
    // When handle is int, the index encode is version 0.
    if (distinct) {
      CodecDataOutput valueCdo = new CodecDataOutput();
      valueCdo.writeLong(handle.intValue());
      return valueCdo.toBytes();
    }
    return new byte[] {'0'};
  }

  private static Boolean needRestoreData(DataType type) {
    if (Collation.isNewCollationEnabled()
        && isNonBinaryStr(type)
        && !(Collation.isBinCollation(type.getCollationCode()) && !isTypeVarChar(type))) {
      return true;
    } else {
      return false;
    }
  }

  private static Boolean tableNeedRestoreData(TiTableInfo tiTableInfo, TiIndexInfo tiIndexInfo) {
    for (TiIndexColumn tiIndexColumn : tiIndexInfo.getIndexColumns()) {
      TiColumnInfo indexColumnInfo = tiTableInfo.getColumn(tiIndexColumn.getOffset());
      if (needRestoreData(indexColumnInfo.getType())) {
        return true;
      }
    }
    return false;
  }

  private static Boolean isNonBinaryStr(DataType type) {
    if (type.getCollationCode() != Collation.translate("binary") && isString(type)) {
      return true;
    } else {
      return false;
    }
  }

  private static boolean isString(DataType type) {
    return isTypeChar(type) || isTypeVarChar(type) || isTypeBlob(type);
  }

  private static Boolean isTypeChar(DataType type) {
    return type.getType() == MySQLType.TypeVarchar || type.getType() == MySQLType.TypeString;
  }

  private static Boolean isTypeBlob(DataType type) {
    return type.getType() == MySQLType.TypeBlob
        || type.getType() == MySQLType.TypeTinyBlob
        || type.getType() == MySQLType.TypeMediumBlob
        || type.getType() == MySQLType.TypeLongBlob;
  }

  private static Boolean isTypeVarChar(DataType type) {
    return type.getType() == MySQLType.TypeVarchar || type.getType() == MySQLType.TypeVarString;
  }

  private static byte[] genIndexValueForCommonHandleVersion1(
      Row row, Handle handle, boolean distinct, TiIndexInfo tiIndexInfo, TiTableInfo tiTableInfo) {
    CodecDataOutput cdo = new CodecDataOutput();
    // add tailLen to cdo, the tailLen is always zero in tispark.
    cdo.writeByte(0);
    cdo.writeByte(IndexVersionFlag);
    cdo.writeByte(1);

    if (distinct) {
      encodeCommonHandle(cdo, handle);
    }

    // encode restore data if needed.
    genRestoreData(row, cdo, tiIndexInfo, tiTableInfo);

    return cdo.toBytes();
  }

  private static void encodeCommonHandle(CodecDataOutput cdo, Handle handle) {
    cdo.write(CommonHandleFlag);
    byte[] encoded = handle.encoded();
    int hLen = encoded.length;
    cdo.writeShort(hLen);
    cdo.write(encoded);
  }

  private static void encodeHandleInUniqueIndexValue(CodecDataOutput cdo, Handle handle) {
    if (handle.isInt()) {
      cdo.writeLong(handle.intValue());
    }
  }

  public static Handle decodeHandleInUniqueIndexValue(byte[] value, boolean isCommonHandle) {
    if (!isCommonHandle) {
      if (value.length <= MaxOldEncodeValueLen) {
        return new IntHandle(new CodecDataInput(value).readLong());
      }
      int tailLen = value[0];
      byte[] encode = Arrays.copyOfRange(value, value.length - tailLen, value.length);
      return new IntHandle(new CodecDataInput(encode).readLong());
    }
    CodecDataInput codecDataInput = new CodecDataInput(value);
    if (getIndexVersion(value) == 1) {
      IndexValueSegments segments = splitIndexValueForCommonHandleVersion1(codecDataInput);
      return new CommonHandle(segments.commonHandle);
    }
    int handleLen = ((int) value[2] << 8) + value[3];
    byte[] encode = Arrays.copyOfRange(value, 4, handleLen + 4);
    return new CommonHandle(encode);
  }

  private static int getIndexVersion(byte[] value) {
    int tailLen = value[0];
    if ((tailLen == 0 || tailLen == 1) && value[1] == IndexVersionFlag) {
      return value[2];
    }
    return 0;
  }

  public static IndexValueSegments splitIndexValueForCommonHandleVersion1(
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
      segments.commonHandle = new byte[handleLen];
      codecDataInput.readFully(segments.commonHandle, 0, handleLen);
    }
    if (codecDataInput.available() > 0 && codecDataInput.peekByte() == PartitionIDFlag) {
      codecDataInput.readByte();
      segments.partitionID = new byte[9];
      codecDataInput.readFully(segments.partitionID, 0, 9);
    }
    if (codecDataInput.available() > 0 && codecDataInput.peekByte() == RestoreDataFlag) {
      codecDataInput.readByte();
      segments.restoredValues = new byte[codecDataInput.available() - tailLen];
      codecDataInput.readFully(segments.restoredValues, 0, codecDataInput.available() - tailLen);
    }
    return segments;
  }
}
