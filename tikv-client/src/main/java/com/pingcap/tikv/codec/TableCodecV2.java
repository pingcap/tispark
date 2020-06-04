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

import com.pingcap.tikv.ExtendedDateTime;
import com.pingcap.tikv.exception.CodecException;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.ObjectRowImpl;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.Converter;
import com.pingcap.tikv.types.DataType;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.joda.time.DateTimeZone;

public class TableCodecV2 {

  /**
   * New Row Format: Reference
   * https://github.com/pingcap/tidb/blob/952d1d7541a8e86be0af58f5b7e3d5e982bab34e/docs/design/2018-07-19-row-format.md
   *
   * <p>- version, flag, numOfNotNullCols, numOfNullCols, notNullCols, nullCols, notNullOffsets,
   * notNullValues
   */
  public static byte[] encodeRow(
      List<TiColumnInfo> columnInfos, Object[] values, boolean isPkHandle)
      throws IllegalAccessException {
    throw new CodecException("not implemented yet");
  }

  public static Row decodeRow(byte[] value, Long handle, TiTableInfo tableInfo) {
    if (handle == null && tableInfo.isPkHandle()) {
      throw new IllegalArgumentException("when pk is handle, handle cannot be null");
    }
    int colSize = tableInfo.getColumns().size();
    // decode bytes to Map<ColumnID, Data>
    HashMap<Long, Object> decodedDataMap = new HashMap<>(colSize);
    RowV2 rowV2 = new RowV2(value);

    for (TiColumnInfo col : tableInfo.getColumns()) {
      if (col.isPrimaryKey() && tableInfo.isPkHandle()) {
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
        Object d = decodeCol(colData, col.getType());
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

  static Object decodeCol(byte[] colData, DataType tp) {
    switch (tp.getType()) {
      case TypeLonglong:
      case TypeLong:
      case TypeInt24:
      case TypeShort:
      case TypeTiny:
        // TODO: decode unsigned
        return decodeInt(colData);
      case TypeYear:
        return decodeInt(colData);
      case TypeFloat:
        return decodeFloat(colData);
      case TypeDouble:
        return decodeDouble(colData);
      case TypeVarString:
      case TypeVarchar:
        return new String(colData, StandardCharsets.UTF_8);
      case TypeString:
      case TypeBlob:
      case TypeTinyBlob:
      case TypeMediumBlob:
      case TypeLongBlob:
        return colData;
      case TypeNewDecimal:
        return decodeDecimal(colData);
      case TypeBit:
        int byteSize = (int) ((tp.getLength() + 7) >>> 3);
        return decodeBit(decodeInt(colData), byteSize);
      case TypeDate:
        return new Date(decodeTimestamp(colData, Converter.getLocalTimezone()).getTime());
      case TypeDatetime:
        return decodeTimestamp(colData, Converter.getLocalTimezone());
      case TypeTimestamp:
        return decodeTimestamp(colData, DateTimeZone.UTC);
      case TypeDuration:
        return decodeInt(colData);
      case TypeEnum:
        throw new CodecException("enum decode not implemented");
      case TypeSet:
        throw new CodecException("set decode not implemented");
      case TypeJSON:
        throw new CodecException("json decode not implemented");
      case TypeDecimal:
      case TypeGeometry:
      case TypeNewDate:
        throw new CodecException("type should not appear in colData");
      case TypeNull:
        throw new CodecException("null should not appear in colData");
      default:
        throw new CodecException("invalid data type " + tp.getType().name());
    }
  }

  static long decodeInt(byte[] val) {
    switch (val.length) {
      case 1:
        return val[0];
      case 2:
        return new CodecDataInputLittleEndian(val).readShort();
      case 4:
        return new CodecDataInputLittleEndian(val).readInt();
      default:
        return new CodecDataInputLittleEndian(val).readLong();
    }
  }

  static float decodeFloat(byte[] val) {
    return (float) decodeDouble(val);
  }

  private static final long signMask = 0x8000000000000000L;

  static double decodeDouble(byte[] val) {
    CodecDataInput cdi = new CodecDataInput(val);
    if (val.length < 8) {
      throw new CodecException("insufficient bytes to decode value");
    }
    long u = cdi.readLong();
    // signMask is less than zero in int64.
    if ((u & signMask) < 0) {
      u &= ~signMask;
    } else {
      u = ~u;
    }
    return Double.longBitsToDouble(u);
  }

  static BigDecimal decodeDecimal(byte[] val) {
    return Codec.DecimalCodec.readDecimal(new CodecDataInputLittleEndian(val));
  }

  private static byte[] trimLeadingZeroBytes(byte[] bytes) {
    if (bytes.length == 0) {
      return bytes;
    }
    int pos = 0, posMax = bytes.length - 1;
    for (; pos < posMax; pos++) {
      if (bytes[pos] != 0) {
        break;
      }
    }
    return Arrays.copyOfRange(bytes, pos, bytes.length);
  }

  static byte[] decodeBit(long val, int byteSize) {
    if (byteSize != -1 && (byteSize < 1 || byteSize > 8)) {
      throw new CodecException("Invalid byteSize " + byteSize);
    }
    CodecDataOutput cdo = new CodecDataOutput();
    cdo.writeLong(val);
    if (byteSize != -1) {
      return trimLeadingZeroBytes(cdo.toBytes());
    } else {
      return Arrays.copyOfRange(cdo.toBytes(), 8 - byteSize, 8);
    }
  }

  static Timestamp decodeTimestamp(byte[] val, DateTimeZone tz) {
    //    Codec.DateTimeCodec.fromPackedLong(, Converter.getLocalTimezone());
    ExtendedDateTime extendedDateTime =
        Codec.DateTimeCodec.fromPackedLong(new CodecDataInputLittleEndian(val).readLong(), tz);
    // Even though null is filtered out but data like 0000-00-00 exists
    // according to MySQL JDBC behavior, it can chose the **ROUND** behavior converted to the
    // nearest
    // value which is 0001-01-01.
    if (extendedDateTime == null) {
      return Codec.DateTimeCodec.createExtendedDateTime(tz, 1, 1, 1, 0, 0, 0, 0).toTimeStamp();
    }
    Timestamp ts = extendedDateTime.toTimeStamp();
    return ts;
  }
}
