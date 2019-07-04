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
  TypeDecimal(0),
  TypeTiny(1),
  TypeShort(2),
  TypeLong(3),
  TypeFloat(4),
  TypeDouble(5),
  TypeNull(6),
  TypeTimestamp(7),
  TypeLonglong(8),
  TypeInt24(9),
  TypeDate(10),
  TypeDuration(11),
  TypeDatetime(12),
  TypeYear(13),
  TypeNewDate(14),
  TypeVarchar(15),
  TypeBit(16),
  TypeJSON(0xf5),
  TypeNewDecimal(0xf6),
  TypeEnum(0xf7),
  TypeSet(0xf8),
  TypeTinyBlob(0xf9),
  TypeMediumBlob(0xfa),
  TypeLongBlob(0xfb),
  TypeBlob(0xfc),
  TypeVarString(0xfd),
  TypeString(0xfe),
  TypeGeometry(0xff);

  private static final Map<Integer, MySQLType> typeMap = new HashMap<>();

  static {
    for (MySQLType type : MySQLType.values()) {
      typeMap.put(type.getTypeCode(), type);
    }
  }

  private int typeCode;

  MySQLType(int tp) {
    typeCode = tp;
  }

  public int getTypeCode() {
    return typeCode;
  }

  public static MySQLType fromTypeCode(int typeCode) {
    MySQLType type = typeMap.get(typeCode);
    return requireNonNull(type, String.format("Cannot find Type from type code %d", typeCode));
  }
}
