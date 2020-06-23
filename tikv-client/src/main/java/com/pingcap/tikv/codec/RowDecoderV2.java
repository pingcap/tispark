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
import com.pingcap.tikv.codec.Codec.DateTimeCodec;
import com.pingcap.tikv.codec.Codec.DecimalCodec;
import com.pingcap.tikv.codec.Codec.EnumCodec;
import com.pingcap.tikv.codec.Codec.SetCodec;
import com.pingcap.tikv.exception.CodecException;
import com.pingcap.tikv.types.Converter;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.JsonUtils;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import org.joda.time.DateTimeZone;

public class RowDecoderV2 {

  private static final long SIGN_MASK = 0x8000000000000000L;

  public static Object decodeCol(byte[] colData, DataType tp) {
    switch (tp.getType()) {
      case TypeLonglong:
      case TypeLong:
      case TypeInt24:
      case TypeShort:
      case TypeTiny:
        // TODO: decode consider unsigned
        return decodeInt(colData);
      case TypeFloat:
        return decodeFloat(colData);
      case TypeDouble:
        return decodeDouble(colData);
      case TypeString:
      case TypeVarString:
      case TypeVarchar:
        return new String(colData, StandardCharsets.UTF_8);
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
      case TypeYear:
        return decodeInt(colData);
      case TypeEnum:
        return decodeEnum(colData, tp.getElems());
      case TypeSet:
        return decodeSet(colData, tp.getElems());
      case TypeJSON:
        return decodeJson(colData);
      case TypeNull:
        return null;
      case TypeDecimal:
      case TypeGeometry:
      case TypeNewDate:
        throw new CodecException("type should not appear in colData");
      default:
        throw new CodecException("invalid data type " + tp.getType().name());
    }
  }

  private static long decodeInt(byte[] val) {
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

  private static float decodeFloat(byte[] val) {
    return (float) decodeDouble(val);
  }

  private static double decodeDouble(byte[] val) {
    CodecDataInput cdi = new CodecDataInput(val);
    if (val.length < 8) {
      throw new CodecException("insufficient bytes to decode value");
    }
    long u = cdi.readLong();
    // signMask is less than zero in int64.
    if ((u & SIGN_MASK) < 0) {
      u &= ~SIGN_MASK;
    } else {
      u = ~u;
    }
    return Double.longBitsToDouble(u);
  }

  private static BigDecimal decodeDecimal(byte[] val) {
    return DecimalCodec.readDecimal(new CodecDataInputLittleEndian(val));
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

  private static byte[] decodeBit(long val, int byteSize) {
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

  private static Timestamp decodeTimestamp(byte[] val, DateTimeZone tz) {
    ExtendedDateTime extendedDateTime =
        DateTimeCodec.fromPackedLong(new CodecDataInputLittleEndian(val).readLong(), tz);
    // Even though null is filtered out but data like 0000-00-00 exists
    // according to MySQL JDBC behavior, it can chose the **ROUND** behavior converted to the
    // nearest
    // value which is 0001-01-01.
    if (extendedDateTime == null) {
      return DateTimeCodec.createExtendedDateTime(tz, 1, 1, 1, 0, 0, 0, 0).toTimeStamp();
    }
    return extendedDateTime.toTimeStamp();
  }

  private static String decodeEnum(byte[] val, List<String> elems) {
    int idx = (int) decodeInt(val) - 1;
    return EnumCodec.readEnumFromIndex(idx, elems);
  }

  private static String decodeSet(byte[] val, List<String> elems) {
    long number = decodeInt(val);
    return SetCodec.readSetFromLong(number, elems);
  }

  private static String decodeJson(byte[] val) {
    return JsonUtils.parseJson(new CodecDataInput(val)).toString();
  }
}
