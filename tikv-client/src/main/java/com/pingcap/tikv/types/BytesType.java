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
import com.pingcap.tikv.meta.TiColumnInfo;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

public class BytesType extends DataType {

  static BytesType of(int tp) {
    return new BytesType(tp);
  }

  protected BytesType(int tp) {
    super(tp);
  }

  protected BytesType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  public String simpleTypeName() { return "binary"; }

  @Override
  public Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag == COMPACT_BYTES_FLAG) {
      return new String(readCompactBytes(cdi));
    } else if (flag == BYTES_FLAG) {
      return new String(readBytes(cdi));
    } else {
      throw new InvalidCodecFormatException("Invalid Flag type for : " + flag);
    }
  }

  /**
   * encode value to cdo per type. If key, then it is memory comparable. If value, no guarantee.
   *
   * @param cdo destination of data.
   * @param encodeType Key or Value.
   * @param value need to be encoded.
   */
  @Override
  public void encodeNotNull(CodecDataOutput cdo, EncodeType encodeType, Object value) {
    byte[] bytes;
    if (value instanceof String) {
      bytes = ((String) value).getBytes();
    } else {
      throw new UnsupportedOperationException("can not cast non String type to String");
    }
    if (encodeType == EncodeType.KEY) {
      cdo.write(BYTES_FLAG);
      writeBytes(cdo, bytes);
    } else {
      cdo.write(COMPACT_BYTES_FLAG);
      writeCompactBytes(cdo, bytes);
    }
  }

  /**
   * get origin default value
   * @param value a bytes string value
   * @return a {@link String} Object
   */
  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return value;
  }

  private static final int GRP_SIZE = 8;
  private static final byte[] PADS = new byte[GRP_SIZE];
  private static final int MARKER = 0xFF;
  private static final byte PAD = (byte) 0x0;

  // writeBytes guarantees the encoded value is in ascending order for comparison,
  // encoding with the following rule:
  //  [group1][marker1]...[groupN][markerN]
  //  group is 8 bytes slice which is padding with 0.
  //  marker is `0xFF - padding 0 count`
  // For example:
  //   [] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
  //   [1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
  //   [1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
  //   [1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
  // Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
  public static void writeBytes(CodecDataOutput cdo, byte[] data) {
    for (int i = 0; i <= data.length; i += GRP_SIZE) {
      int remain = data.length - i;
      int padCount = 0;
      if (remain >= GRP_SIZE) {
        cdo.write(data, i, GRP_SIZE);
      } else {
        padCount = GRP_SIZE - remain;
        cdo.write(data, i, data.length - i);
        cdo.write(PADS, 0, padCount);
      }
      cdo.write((byte) (MARKER - padCount));
    }
  }

  /**
   * Write bytes in a compact form.
   *
   * @param cdo destination of data.
   * @param data is value that will be written into cdo.
   */
  public static void writeCompactBytes(CodecDataOutput cdo, byte[] data) {
    int length = data.length;
    IntegerType.writeVarLong(cdo, length);
    cdo.writeBytes(Arrays.toString(data));
  }

  // WriteBytesDesc first encodes bytes using EncodeBytes, then bitwise reverses
  // encoded value to guarantee the encoded value is in descending order for comparison.
  public static void writeBytesDesc(CodecDataOutput cdo, byte[] data) {
    writeBytes(cdo, data);
    byte[] encodedData = cdo.toBytes();
    cdo.reset();
    writeBytes(cdo, reverseBytes(encodedData));
  }

  private static byte[] reverseBytes(byte[] data) {
    for (int i = 0; i < data.length; i++) {
      data[i] ^= data[i];
    }
    return data;
  }

  // readBytes decodes bytes which is encoded by EncodeBytes before,
  // returns the leftover bytes and decoded value if no error.
  public static byte[] readBytes(CodecDataInput cdi) {
    return readBytes(cdi, false);
  }

  public static byte[] readCompactBytes(CodecDataInput cdi) {
    int size = (int) IntegerType.readVarLong(cdi);
    return readCompactBytes(cdi, size);
  }

  private static byte[] readCompactBytes(CodecDataInput cdi, int size) {
    byte[] data = new byte[size];
    for (int i = 0; i < size; i++) {
      data[i] = cdi.readByte();
    }
    return data;
  }

  public static byte[] readBytesDesc(CodecDataInput cdi) {
    return readBytes(cdi, true);
  }

  private static byte[] readBytes(CodecDataInput cdi, boolean reverse) {
    CodecDataOutput cdo = new CodecDataOutput();
    while (true) {
      byte[] groupBytes = new byte[GRP_SIZE + 1];

      cdi.readFully(groupBytes, 0, GRP_SIZE + 1);
      byte[] group = Arrays.copyOfRange(groupBytes, 0, GRP_SIZE);

      int padCount;
      int marker = Byte.toUnsignedInt(groupBytes[GRP_SIZE]);

      if (reverse) {
        padCount = marker;
      } else {
        padCount = MARKER - marker;
      }

      checkArgument(padCount <= GRP_SIZE);
      int realGroupSize = GRP_SIZE - padCount;
      cdo.write(group, 0, realGroupSize);

      if (padCount != 0) {
        byte padByte = PAD;
        if (reverse) {
          padByte = (byte) MARKER;
        }
        // Check validity of padding bytes.
        for (int i = realGroupSize; i < group.length; i++) {
          byte b = group[i];
          checkArgument(padByte == b);
        }
        break;
      }
    }
    byte[] bytes = cdo.toBytes();
    if (reverse) {
      for (int i = 0; i < bytes.length; i++) {
        bytes[i] = (byte) ~bytes[i];
      }
    }
    return bytes;
  }
}
