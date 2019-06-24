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
  TypeDecimal(0, 16, 1),
  TypeTiny(1, 1, 1),
  TypeShort(2, 2, 1),
  TypeLong(3, 4, 1),
  TypeFloat(4, 4, 1),
  TypeDouble(5, 8, 1),
  TypeNull(6, 8, 1),
  TypeTimestamp(7, 32, 1),
  TypeLonglong(8, 8, 1),
  TypeInt24(9, 3, 1),
  TypeDate(10, 3, 1),
  TypeDuration(11, 8, 1),
  TypeDatetime(12, 8, 1),
  TypeYear(13, 8, 1),
  TypeNewDate(14, 8, 1),
  TypeVarchar(15, 256, 2),
  TypeBit(16, 1, 1),
  TypeJSON(0xf5, 256, 1),
  TypeNewDecimal(0xf6, 32, 1),
  TypeEnum(0xf7, 8, 1),
  TypeSet(0xf8, 8, 1),
  TypeTinyBlob(0xf9, 256, 2),
  TypeMediumBlob(0xfa, 21777216, 3),
  TypeLongBlob(0xfb, 4294967296L, 4),
  TypeBlob(0xfc, 65536, 2),
  TypeVarString(0xfd, 256, 1),
  TypeString(0xfe, 256, 1),
  TypeGeometry(0xff, 256, 1);

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

  private int typeCode;
  private long defaultSize;
  private int prefixSize;

  MySQLType(int tp, long sz, int lengthSz) {
    typeCode = tp;
    defaultSize = sz;
    prefixSize = lengthSz;
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

  public static MySQLType fromTypeCode(int typeCode) {
    MySQLType type = typeMap.get(typeCode);
    return requireNonNull(type, String.format("Cannot find Type from type code %d", typeCode));
  }

  public static long getTypeDefaultSize(int typeCode) {
    Long size = sizeMap.get(typeCode);
    return requireNonNull(
        size, String.format("Cannot find default size from type code %d", typeCode));
  }
}
