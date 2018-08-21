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


import com.pingcap.tikv.exception.InvalidCodecFormatException;
import gnu.trove.list.array.TIntArrayList;
import org.joda.time.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

public class Codec {

  public static final int NULL_FLAG = 0;
  public static final int BYTES_FLAG = 1;
  public static final int COMPACT_BYTES_FLAG = 2;
  public static final int INT_FLAG = 3;
  public static final int UINT_FLAG = 4;
  public static final int FLOATING_FLAG = 5;
  public static final int DECIMAL_FLAG = 6;
  public static final int DURATION_FLAG = 7;
  public static final int VARINT_FLAG = 8;
  public static final int UVARINT_FLAG = 9;
  public static final int JSON_FLAG = 10;
  public static final int MAX_FLAG = 250;

  public static boolean isNullFlag(int flag) {
    return flag == NULL_FLAG;
  }


  public static class IntegerCodec {
    private static final long SIGN_MASK = ~Long.MAX_VALUE;

    private static long flipSignBit(long v) {
      return v ^ SIGN_MASK;
    }

    /**
     * Encoding a long value to byte buffer with type flag at the beginning
     *
     * @param cdo For outputting data in bytes array
     * @param lVal The data to encode
     * @param comparable If the output should be memory comparable without decoding. In real TiDB use
     *     case, if used in Key encoding, we output memory comparable format otherwise not
     */
    public static void writeLongFully(CodecDataOutput cdo, long lVal, boolean comparable) {
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
    public static void writeULongFully(CodecDataOutput cdo, long lVal, boolean comparable) {
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
      cdo.writeLong(flipSignBit(lVal));
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
     * Decode as signed long, assuming encoder flips signed bit for memory comparable
     *
     * @param cdi source of data
     * @return decoded signed long value
     */
    public static long readLong(CodecDataInput cdi) {
      return flipSignBit(cdi.readLong());
    }

    public static long readPartialLong(CodecDataInput cdi) {
      return flipSignBit(cdi.readPartialLong());
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
    public static long readVarLong(CodecDataInput cdi) {
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
  }

  public static class BytesCodec {

    private static final int GRP_SIZE = 8;
    private static final byte[] PADS = new byte[GRP_SIZE];
    private static final int MARKER = 0xFF;
    private static final byte PAD = (byte) 0x0;

    public static void writeBytesRaw(CodecDataOutput cdo, byte[] data) {
      cdo.write(data);
    }

    public static void writeBytesFully(CodecDataOutput cdo, byte[] data) {
      cdo.write(Codec.BYTES_FLAG);
      BytesCodec.writeBytes(cdo, data);
    }

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

    public static void writeCompactBytesFully(CodecDataOutput cdo, byte[] data) {
      cdo.write(Codec.COMPACT_BYTES_FLAG);
      writeCompactBytes(cdo, data);
    }

    /**
     * Write bytes in a compact form.
     *
     * @param cdo destination of data.
     * @param data is value that will be written into cdo.
     */
    public static void writeCompactBytes(CodecDataOutput cdo, byte[] data) {
      int length = data.length;
      IntegerCodec.writeVarLong(cdo, length);
      cdo.write(data);
    }

    // readBytes decodes bytes which is encoded by EncodeBytes before,
    // returns the leftover bytes and decoded value if no error.
    public static byte[] readBytes(CodecDataInput cdi) {
      return readBytes(cdi, false);
    }

    public static byte[] readCompactBytes(CodecDataInput cdi) {
      int size = (int) IntegerCodec.readVarLong(cdi);
      return readCompactBytes(cdi, size);
    }

    private static byte[] readCompactBytes(CodecDataInput cdi, int size) {
      byte[] data = new byte[size];
      for (int i = 0; i < size; i++) {
        data[i] = cdi.readByte();
      }
      return data;
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

  public static class RealCodec {

    private static final long signMask = 0x8000000000000000L;

    /**
     * Decode as float
     *
     * @param cdi source of data
     * @return decoded unsigned long value
     */
    public static double readDouble(CodecDataInput cdi) {
      long u = IntegerCodec.readULong(cdi);
      if (u < 0) {
        u &= Long.MAX_VALUE;
      } else {
        u = ~u;
      }
      return Double.longBitsToDouble(u);
    }

    private static long encodeDoubleToCmpLong(double val) {
      long u = Double.doubleToRawLongBits(val);
      if (val >= 0) {
        u |= signMask;
      } else {
        u = ~u;
      }
      return u;
    }

    public static void writeDoubleFully(CodecDataOutput cdo, double val) {
      cdo.writeByte(FLOATING_FLAG);
      writeDouble(cdo, val);
    }

    /**
     * Encoding a double value to byte buffer
     *
     * @param cdo For outputting data in bytes array
     * @param val The data to encode
     */
    public static void writeDouble(CodecDataOutput cdo, double val) {
      IntegerCodec.writeULong(cdo, encodeDoubleToCmpLong(val));
    }
  }

  public static class DecimalCodec {

    /**
     * read a decimal value from CodecDataInput
     *
     * @param cdi cdi is source data.
     */
    public static BigDecimal readDecimal(CodecDataInput cdi) {
      if (cdi.available() < 3) {
        throw new IllegalArgumentException("insufficient bytes to read value");
      }

      // 64 should be larger enough for avoiding unnecessary growth.
      TIntArrayList data = new TIntArrayList(64);
      int precision = cdi.readUnsignedByte();
      int frac = cdi.readUnsignedByte();
      int length = precision + frac;
      int curPos = cdi.size() - cdi.available();
      for (int i = 0; i < length; i++) {
        if (cdi.eof()) {
          break;
        }
        data.add(cdi.readUnsignedByte());
      }

      MyDecimal dec = new MyDecimal();
      int binSize = dec.fromBin(precision, frac, data.toArray());
      cdi.mark(curPos + binSize);
      cdi.reset();
      return dec.toDecimal();
    }

    /**
     * write a decimal value from CodecDataInput
     *
     * @param cdo cdo is destination data.
     * @param dec is decimal value that will be written into cdo.
     */
    static void writeDecimal(CodecDataOutput cdo, MyDecimal dec) {
      int[] data = dec.toBin(dec.precision(), dec.frac());
      cdo.writeByte(dec.precision());
      cdo.writeByte(dec.frac());
      for (int aData : data) {
        cdo.writeByte(aData & 0xFF);
      }
    }

    public static void writeDecimalFully(CodecDataOutput cdo, BigDecimal val) {
      cdo.writeByte(DECIMAL_FLAG);
      writeDecimal(cdo, val);
    }

    /**
     * Encoding a double value to byte buffer
     *
     * @param cdo For outputting data in bytes array
     * @param val The data to encode
     */
    public static void writeDecimal(CodecDataOutput cdo, BigDecimal val) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(val.toPlainString());
      writeDecimal(cdo, dec);
    }
  }

  public static class DateTimeCodec {

    /**
     * Encode a DateTime to a packed long converting to specific timezone
     *
     * @param dateTime dateTime that need to be encoded.
     * @param tz timezone used for converting to localDateTime
     * @return a packed long.
     */
    static long toPackedLong(DateTime dateTime, DateTimeZone tz) {
      LocalDateTime localDateTime = dateTime.withZone(tz).toLocalDateTime();
      return toPackedLong(localDateTime.getYear(),
          localDateTime.getMonthOfYear(),
          localDateTime.getDayOfMonth(),
          localDateTime.getHourOfDay(),
          localDateTime.getMinuteOfHour(),
          localDateTime.getSecondOfMinute(),
          localDateTime.getMillisOfSecond() * 1000);
    }

    /**
     * Encode a UTC Date to a packed long converting to specific timezone
     *
     * @param date date that need to be encoded.
     * @param tz timezone used for converting to localDate
     * @return a packed long.
     */
    static long toPackedLong(Date date, DateTimeZone tz) {
      return toPackedLong(date.getTime(), tz);
    }

    static long toPackedLong(long utcMillsTs, DateTimeZone tz) {
      LocalDate date = new LocalDate(utcMillsTs, tz);
      return toPackedLong(date);
    }

    static long toPackedLong(LocalDate date) {
      return Codec.DateTimeCodec.toPackedLong(
          date.getYear(),
          date.getMonthOfYear(),
          date.getDayOfMonth(),
          0, 0, 0, 0
      );
    }

    /**
     * Encode a date/time parts to a packed long.
     *
     * @return a packed long.
     */
    static long toPackedLong(int year, int month, int day, int hour, int minute,
        int second, int micro) {
      long ymd = (year * 13 + month) << 5 | day;
      long hms = hour << 12 | minute << 6 | second;
      return ((ymd << 17 | hms) << 24) | micro;
    }

    /**
     * Read datetime from packed Long which contains all parts of a datetime
     * namely, year, month, day and hour, min and sec, millisec.
     * The original representation does not indicate any timezone information
     * In Timestamp type, it should be interpreted as UTC while in DateType
     * it is interpreted as local timezone
     *
     * @param packed long value that packs date / time parts
     * @param tz timezone to interpret datetime parts
     * @return decoded DateTime using provided timezone
     */
    static DateTime fromPackedLong(long packed, DateTimeZone tz) {
      // TODO: As for JDBC behavior, it can be configured to "round" or "toNull"
      // for now we didn't pass in session so we do a toNull behavior
      if (packed == 0) {
        return null;
      }
      long ymdhms = packed >> 24;
      long ymd = ymdhms >> 17;
      int day = (int) (ymd & ((1 << 5) - 1));
      long ym = ymd >> 5;
      int month = (int) (ym % 13);
      int year = (int) (ym / 13);

      int hms = (int) (ymdhms & ((1 << 17) - 1));
      int second = hms & ((1 << 6) - 1);
      int minute = (hms >> 6) & ((1 << 6) - 1);
      int hour = hms >> 12;
      int microsec = (int) (packed % (1 << 24));

      try {
        return new DateTime(year, month, day, hour, minute, second, microsec / 1000, tz);
      } catch (IllegalInstantException e) {
        LocalDateTime localDateTime = new LocalDateTime(
            year, month, day, hour,
            minute, second, microsec / 1000);
        DateTime dt = localDateTime.toLocalDate().toDateTimeAtStartOfDay(tz);
        long millis = dt.getMillis() + localDateTime.toLocalTime().getMillisOfDay();
        return new DateTime(millis, tz);
      }
    }

    /**
     * Encode DateTime as packed long converting into specified timezone
     * All timezone conversion should be done beforehand
     * @param cdo encoding output
     * @param dateTime value to encode
     * @param tz timezone used to converting local time
     */
    public static void writeDateTimeFully(CodecDataOutput cdo, DateTime dateTime, DateTimeZone tz) {
      long val = DateTimeCodec.toPackedLong(dateTime, tz);
      IntegerCodec.writeULongFully(cdo, val, true);
    }

    /**
     * Encode DateTime as packed long converting into specified timezone
     * All timezone conversion should be done beforehand
     * The encoded value has no data type flag
     * @param cdo encoding output
     * @param dateTime value to encode
     * @param tz timezone used to converting local time
     */
    public static void writeDateTimeProto(CodecDataOutput cdo, DateTime dateTime, DateTimeZone tz) {
      long val = DateTimeCodec.toPackedLong(dateTime, tz);
      IntegerCodec.writeULong(cdo, val);
    }

    /**
     * Encode Date as packed long converting into specified timezone
     * All timezone conversion should be done beforehand
     * @param cdo encoding output
     * @param date value to encode
     * @param tz timezone used to converting local time
     */
    public static void writeDateFully(CodecDataOutput cdo, Date date, DateTimeZone tz) {
      long val = DateTimeCodec.toPackedLong(date, tz);
      IntegerCodec.writeULongFully(cdo, val, true);
    }

    /**
     * Encode Date as packed long converting into specified timezone
     * All timezone conversion should be done beforehand
     * The encoded value has no data type flag
     * @param cdo encoding output
     * @param date value to encode
     * @param tz timezone used to converting local time
     */
    public static void writeDateProto(CodecDataOutput cdo, Date date, DateTimeZone tz) {
      long val = DateTimeCodec.toPackedLong(date, tz);
      IntegerCodec.writeULong(cdo, val);
    }

    /**
     * Read datetime from packed Long encoded as unsigned var-len integer
     * converting into specified timezone
     * @see DateTimeCodec#fromPackedLong(long, DateTimeZone)
     *
     * @param cdi codec buffer input
     * @param tz timezone to interpret datetime parts
     * @return decoded DateTime using provided timezone
     */
    public static DateTime readFromUVarInt(CodecDataInput cdi, DateTimeZone tz) {
      return DateTimeCodec.fromPackedLong(IntegerCodec.readUVarLong(cdi), tz);
    }

    /**
     * Read datetime from packed Long as unsigned fixed-len integer
     * @see DateTimeCodec#fromPackedLong(long, DateTimeZone)
     *
     * @param cdi codec buffer input
     * @param tz timezone to interpret datetime parts
     * @return decoded DateTime using provided timezone
     */
    public static DateTime readFromUInt(CodecDataInput cdi, DateTimeZone tz) {
      return DateTimeCodec.fromPackedLong(IntegerCodec.readULong(cdi), tz);
    }
  }
}
