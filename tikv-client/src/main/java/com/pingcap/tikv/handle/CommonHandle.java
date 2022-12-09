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

package com.pingcap.tikv.handle;

import com.pingcap.tikv.codec.Codec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.MySQLType;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.tikv.common.exception.CodecException;
import org.tikv.common.util.FastByteComparisons;

public class CommonHandle implements Handle {
  private final byte[] encoded;
  private final int[] colEndOffsets;

  private static final int MS_OF_ONE_DAY = 24 * 3600 * 1000;
  private static final int MIN_ENCODE_LEN = 9;

  public static CommonHandle newCommonHandle(DataType[] dataTypes, Object[] data) {
    long[] prefixLengthes = new long[dataTypes.length];
    for (int i = 0; i < dataTypes.length; i++) {
      prefixLengthes[i] = -1;
    }
    return newCommonHandle(dataTypes, data, prefixLengthes);
  }

  public static CommonHandle newCommonHandle(
      DataType[] dataTypes, Object[] data, long[] prefixLengthes) {
    CodecDataOutput cdo = new CodecDataOutput();
    for (int i = 0; i < data.length; i++) {
      if (dataTypes[i].getType().equals(MySQLType.TypeTimestamp)) {
        // When writing `Timestamp`, it will pass `Timestamp` object.
        // When indexScan or tableScan, it will pass `Long` object.
        // It's a compromise here since we don't have a good way to make them consistent.
        long milliseconds;
        if (data[i] instanceof Timestamp) {
          milliseconds = ((Timestamp) data[i]).getTime();
        } else {
          milliseconds = ((long) data[i]) / 1000;
        }

        dataTypes[i].encode(cdo, DataType.EncodeType.KEY, milliseconds);
      } else if (dataTypes[i].getType().equals(MySQLType.TypeDate)) {
        long days;
        // When writing `Date`, it will pass `Date` object.
        // When indexScan or tableScan, it will pass `Long` object.
        // It's a compromise here since we don't have a good way to make them consistent.
        if (data[i] instanceof Date) {
          days = Days.daysBetween(new LocalDate(1970, 1, 1), new LocalDate(data[i])).getDays();
        } else {
          days = (long) data[i];
        }

        SimpleDateFormat utcFmt = new SimpleDateFormat("yyyy-MM-dd");
        utcFmt.setTimeZone(TimeZone.getTimeZone("UTC"));

        dataTypes[i].encode(
            cdo, DataType.EncodeType.KEY, Date.valueOf(utcFmt.format(days * MS_OF_ONE_DAY)));
      } else {
        if (prefixLengthes[i] > 0 && data[i] instanceof String) {
          String source = (String) data[i];
          String dest = source;
          if (source.length() > prefixLengthes[i]) {
            dest = source.substring(0, (int) prefixLengthes[i]);
          }
          dataTypes[i].encode(cdo, DataType.EncodeType.KEY, dest);
        } else {
          dataTypes[i].encode(cdo, DataType.EncodeType.KEY, data[i]);
        }
      }
    }
    return new CommonHandle(cdo.toBytes());
  }

  public CommonHandle(byte[] encoded) {
    if (encoded.length < MIN_ENCODE_LEN) {
      this.encoded = Arrays.copyOf(encoded, MIN_ENCODE_LEN);
    } else {
      this.encoded = encoded;
    }

    int endOffset = 0;
    CodecDataInput cdi = new CodecDataInput(encoded);
    List<Integer> offsets = new ArrayList<>();
    while (!cdi.eof()) {
      if (cdi.peekByte() == 0) {
        // padded data
        break;
      }
      endOffset += cdi.cutOne();
      offsets.add(endOffset);
    }
    this.colEndOffsets = offsets.stream().mapToInt(i -> i).toArray();
  }

  public CommonHandle(byte[] encoded, int[] colEndOffsets) {
    if (encoded.length < MIN_ENCODE_LEN) {
      this.encoded = Arrays.copyOf(encoded, MIN_ENCODE_LEN);
    } else {
      this.encoded = encoded;
    }
    this.colEndOffsets = colEndOffsets;
  }

  @Override
  public boolean isInt() {
    return false;
  }

  @Override
  public long intValue() {
    throw new CodecException("not supported in CommonHandle");
  }

  @Override
  public Handle next() {
    return new CommonHandle(new Key(encoded).nextPrefix().getBytes(), colEndOffsets);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof CommonHandle) {
      return Arrays.equals(encoded, ((CommonHandle) other).encoded());
    }
    return false;
  }

  @Override
  public int compare(Handle h) {
    if (h.isInt()) {
      throw new RuntimeException("CommonHandle compares to IntHandle");
    }
    return FastByteComparisons.compareTo(encoded, h.encoded());
  }

  @Override
  public byte[] encoded() {
    return this.encoded;
  }

  @Override
  public byte[] encodedAsKey() {
    return this.encoded;
  }

  @Override
  public int len() {
    return this.encoded.length;
  }

  @Override
  public int numCols() {
    return this.colEndOffsets.length;
  }

  @Override
  public byte[] encodedCol(int idx) {
    int start = 0, end = colEndOffsets[idx];
    if (idx > 0) {
      start = colEndOffsets[idx - 1];
    }
    return Arrays.copyOfRange(encoded, start, end + 1);
  }

  @Override
  public Object[] data() {
    int len = numCols();
    Object[] data = new Object[len];
    for (int i = 0; i < len; i++) {
      byte[] col = encodedCol(i);
      data[i] = Codec.decodeOne(col);
    }
    return data;
  }

  @Override
  public String toString() {
    Object[] data = data();
    return Arrays.stream(data).map(Object::toString).collect(Collectors.joining("},{", "{", "}"));
  }
}
