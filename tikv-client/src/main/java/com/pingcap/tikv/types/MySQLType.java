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

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;

public enum MySQLType {
  TypeDecimal(0, 16, 1, 11),
  TypeTiny(1, 1, 1, 4),
  TypeShort(2, 2, 1, 6),
  TypeLong(3, 4, 1, 11),
  TypeFloat(4, 4, 1, -1),
  TypeDouble(5, 8, 1, -1),
  TypeNull(6, 8, 1, -1),
  TypeTimestamp(7, 32, 1, -1),
  TypeLonglong(8, 8, 1, 20),
  TypeInt24(9, 3, 1, 9),
  TypeDate(10, 3, 1, -1),
  // TypeDuration is just MySQL time type.
  // MySQL uses the 'HHH:MM:SS' format, which is larger than 24 hours.
  TypeDuration(11, 8, 1, -1),
  TypeDatetime(12, 8, 1, -1),
  TypeYear(13, 8, 1, 4),
  TypeNewDate(14, 8, 1, -1),
  TypeVarchar(15, 255, 2, -1),
  TypeBit(16, 1, 1, 1),
  TypeJSON(0xf5, 1024, 1, -1),
  TypeNewDecimal(0xf6, 32, 1, 11),
  TypeEnum(0xf7, 8, 1, -1),
  TypeSet(0xf8, 8, 1, -1),
  TypeTinyBlob(0xf9, 255, 2, -1),
  TypeMediumBlob(0xfa, 21777215, 3, -1),
  TypeLongBlob(0xfb, 4294967295L, 4, -1),
  TypeBlob(0xfc, 65535, 2, -1),
  TypeVarString(0xfd, 255, 1, -1),
  TypeString(0xfe, 255, 1, 1),
  TypeGeometry(0xff, 1024, 1, -1);

  private static final Map<Integer, MySQLType> typeMap = new HashMap<>();
  private static final Map<Integer, Long> sizeMap = new HashMap<>();

  static {
    for (MySQLType type : MySQLType.values()) {
      typeMap.put(type.getTypeCode(), type);
    }
    for (MySQLType type : MySQLType.values()) {
      sizeMap.put(type.getTypeCode(), type.defaultSize);
    }
  }

  private final int typeCode;
  private final long defaultSize;
  private final int prefixSize;
  private final int defaultLength;

  MySQLType(int tp, long sz, int lengthSz, int M) {
    typeCode = tp;
    defaultSize = sz;
    prefixSize = lengthSz;
    defaultLength = M;
  }

  public static MySQLType fromTypeCode(int typeCode) {
    MySQLType type = typeMap.get(typeCode);
    return requireNonNull(type, String.format("Cannot find Type from type code %d", typeCode));
  }

  public static long getTypeDefaultSize(int typeCode) {
    Long size = sizeMap.get(typeCode);
    return requireNonNull(
        size, String.format("Cannot find default size from type code %d", typeCode));
  }

  public int getTypeCode() {
    return typeCode;
  }

  public long getDefaultSize() {
    return defaultSize;
  }

  public long getPrefixSize() {
    return prefixSize;
  }

  public int getDefaultLength() {
    return defaultLength;
  }
}
