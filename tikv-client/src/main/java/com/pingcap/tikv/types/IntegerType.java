/*
 *
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
 *
 */

package com.pingcap.tikv.types;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.InvalidCodecFormatException;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.TiColumnInfo;

import static com.pingcap.tikv.types.Types.*;

/** Base class for all integer types: Tiny, Short, Medium, Int, Long and LongLong */
public class IntegerType extends DataType {
  public static final IntegerType DEF_LONG_TYPE = new IntegerType(Types.TYPE_LONG);
  public static final IntegerType DEF_LONG_LONG_TYPE = new IntegerType(Types.TYPE_LONG_LONG);

  static IntegerType of(int tp) {
    return new IntegerType(tp);
  }

  protected IntegerType(int tp) {
    super(tp);
  }

  @Override
  public Object decodeNotNull(int flag, CodecDataInput cdi) {
    return decodeNotNullPrimitive(flag, cdi);
  }

  public static long decodeNotNullPrimitive(int flag, CodecDataInput cdi) {
    switch (flag) {
      case UVARINT_FLAG:
        return readUVarLong(cdi);
      case UINT_FLAG:
        return readULong(cdi);
      case VARINT_FLAG:
        return readVarLong(cdi);
      case INT_FLAG:
        return readLong(cdi);
      default:
        throw new TiClientInternalException("Invalid IntegerType flag: " + flag);
    }
  }

  /**
   * Encode a value to cdo.
   *
   * @param cdo destination of data.
   * @param encodeType Key or Value.
   * @param value need to be encoded.
   */
  @Override
  public void encodeNotNull(CodecDataOutput cdo, EncodeType encodeType, Object value) {
    long val;
    if (value instanceof Number) {
      val = ((Number) value).longValue();
    } else {
      throw new UnsupportedOperationException("Cannot cast Un-number value to long");
    }
    boolean comparable = false;
    if (encodeType == EncodeType.KEY) {
      comparable = true;
    }
    switch (tp) {
      case TYPE_SHORT:
      case TYPE_INT24:
      case TYPE_LONG:
        writeLongFull(cdo, val, comparable);
        break;
      case TYPE_LONG_LONG:
        writeULongFull(cdo, val, comparable);
        break;
    }
  }

  /**
   * get origin default value
   * @param value a int value represents in string
   * @return a int object
   */
  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return Integer.parseInt(value);
  }

  protected IntegerType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /**
   * Encoding a long value to byte buffer with type flag at the beginning
   *
   * @param cdo For outputting data in bytes array
   * @param lVal The data to encode
   * @param comparable If the output should be memory comparable without decoding. In real TiDB use
   *     case, if used in Key encoding, we output memory comparable format otherwise not
   */
  public static void writeLongFull(CodecDataOutput cdo, long lVal, boolean comparable) {
    if (comparable) {
      cdo.writeByte(INT_FLAG);
      writeLong(cdo, lVal);
    } else {
      cdo.writeByte(VARINT_FLAG);
      writeVarLong(cdo, lVal);
    }
  }

  /**
   * Encoding a unsigned long value to byte buffer with type flag at the beginning
   *
   * @param cdo For outputting data in bytes array
   * @param lVal The data to encode, note that long is treated as unsigned
   * @param comparable If the output should be memory comparable without decoding. In real TiDB use
   *     case, if used in Key encoding, we output memory comparable format otherwise not
   */
  public static void writeULongFull(CodecDataOutput cdo, long lVal, boolean comparable) {
    if (comparable) {
      cdo.writeByte(UINT_FLAG);
      writeULong(cdo, lVal);
    } else {
      cdo.writeByte(UVARINT_FLAG);
      writeUVarLong(cdo, lVal);
    }
  }

  /**
   * Encode long value without type flag at the beginning The signed bit is flipped for memory
   * comparable purpose
   *
   * @param cdo For outputting data in bytes array
   * @param lVal The data to encode
   */
  public static void writeLong(CodecDataOutput cdo, long lVal) {
    cdo.writeLong(TableCodec.flipSignBit(lVal));
  }

  /**
   * Encode long value without type flag at the beginning
   *
   * @param cdo For outputting data in bytes array
   * @param lVal The data to encode
   */
  public static void writeULong(CodecDataOutput cdo, long lVal) {
    cdo.writeLong(lVal);
  }

  /**
   * Encode var-length long, same as go's binary.PutVarint
   *
   * @param cdo For outputting data in bytes array
   * @param value The data to encode
   */
  public static void writeVarLong(CodecDataOutput cdo, long value) {
    long ux = value << 1;
    if (value < 0) {
      ux = ~ux;
    }
    writeUVarLong(cdo, ux);
  }

  /**
   * Encode Data as var-length long, the same as go's binary.PutUvarint
   *
   * @param cdo For outputting data in bytes array
   * @param value The data to encode
   */
  public static void writeUVarLong(CodecDataOutput cdo, long value) {
    while ((value - 0x80) >= 0) {
      cdo.writeByte((byte) value | 0x80);
      value >>>= 7;
    }
    cdo.writeByte((byte) value);
  }

  /**
   * Decode data as signed long from CodecDataInput assuming type flag at the beginning
   *
   * @param cdi source of data
   * @return value decoded
   * @exception InvalidCodecFormatException wrong format of binary encoding encountered
   */
  public static long readLongFully(CodecDataInput cdi) {
    int flag = cdi.readByte();

    switch (flag) {
      case INT_FLAG:
        return readLong(cdi);
      case VARINT_FLAG:
        return readVarLong(cdi);
      default:
        throw new InvalidCodecFormatException("Invalid Flag type for signed long type: " + flag);
    }
  }

  /**
   * Decode data as unsigned long from CodecDataInput assuming type flag at the beginning
   *
   * @param cdi source of data
   * @return value decoded
   * @exception InvalidCodecFormatException wrong format of binary encoding encountered
   */
  public static long readULongFully(CodecDataInput cdi) {
    byte flag = cdi.readByte();
    switch (flag) {
      case UINT_FLAG:
        return readULong(cdi);
      case UVARINT_FLAG:
        return readUVarLong(cdi);
      default:
        throw new InvalidCodecFormatException("Invalid Flag type for unsigned long type: " + flag);
    }
  }

  /**
   * Decode as signed long, assuming encoder flips signed bit for memory comparable
   *
   * @param cdi source of data
   * @return decoded signed long value
   */
  public static long readLong(CodecDataInput cdi) {
    return TableCodec.flipSignBit(cdi.readLong());
  }

  public static long readPartialLong(CodecDataInput cdi) {
    return TableCodec.flipSignBit(cdi.readPartialLong());
  }

  /**
   * Decode as unsigned long without any binary manipulation
   *
   * @param cdi source of data
   * @return decoded unsigned long value
   */
  public static long readULong(CodecDataInput cdi) {
    return cdi.readLong();
  }

  /**
   * Decode as var-length long, the same as go's binary.Varint
   *
   * @param cdi source of data
   * @return decoded signed long value
   */
  static long readVarLong(CodecDataInput cdi) {
    long ux = readUVarLong(cdi);
    long x = ux >>> 1;
    if ((ux & 1) != 0) {
      x = ~x;
    }
    return x;
  }

  /**
   * Decode as var-length unsigned long, the same as go's binary.Uvarint
   *
   * @param cdi source of data
   * @return decoded unsigned long value
   */
  public static long readUVarLong(CodecDataInput cdi) {
    long x = 0;
    int s = 0;
    for (int i = 0; !cdi.eof(); i++) {
      long b = cdi.readUnsignedByte();
      if ((b - 0x80) < 0) {
        if (i > 9 || i == 9 && b > 1) {
          throw new InvalidCodecFormatException("readUVarLong overflow");
        }
        return x | b << s;
      }
      x |= (b & 0x7f) << s;
      s += 7;
    }
    throw new InvalidCodecFormatException("readUVarLong encountered unfinished data");
  }

  public static final IntegerType DEF_BOOLEAN_TYPE = new IntegerType(Types.TYPE_SHORT);
}
