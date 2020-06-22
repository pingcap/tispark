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

package com.pingcap.tikv.expression;

import static com.pingcap.tikv.types.MySQLType.TypeBlob;
import static com.pingcap.tikv.types.MySQLType.TypeDate;
import static com.pingcap.tikv.types.MySQLType.TypeDatetime;
import static com.pingcap.tikv.types.MySQLType.TypeDecimal;
import static com.pingcap.tikv.types.MySQLType.TypeDouble;
import static com.pingcap.tikv.types.MySQLType.TypeDuration;
import static com.pingcap.tikv.types.MySQLType.TypeEnum;
import static com.pingcap.tikv.types.MySQLType.TypeFloat;
import static com.pingcap.tikv.types.MySQLType.TypeInt24;
import static com.pingcap.tikv.types.MySQLType.TypeJSON;
import static com.pingcap.tikv.types.MySQLType.TypeLong;
import static com.pingcap.tikv.types.MySQLType.TypeLongBlob;
import static com.pingcap.tikv.types.MySQLType.TypeLonglong;
import static com.pingcap.tikv.types.MySQLType.TypeMediumBlob;
import static com.pingcap.tikv.types.MySQLType.TypeNewDate;
import static com.pingcap.tikv.types.MySQLType.TypeNewDecimal;
import static com.pingcap.tikv.types.MySQLType.TypeNull;
import static com.pingcap.tikv.types.MySQLType.TypeSet;
import static com.pingcap.tikv.types.MySQLType.TypeShort;
import static com.pingcap.tikv.types.MySQLType.TypeString;
import static com.pingcap.tikv.types.MySQLType.TypeTimestamp;
import static com.pingcap.tikv.types.MySQLType.TypeTiny;
import static com.pingcap.tikv.types.MySQLType.TypeTinyBlob;
import static com.pingcap.tikv.types.MySQLType.TypeVarString;
import static com.pingcap.tikv.types.MySQLType.TypeVarchar;
import static com.pingcap.tikv.types.MySQLType.TypeYear;

import com.pingcap.tikv.types.MySQLType;
import java.util.HashMap;
import java.util.Map;

public class TypeBlocklist extends Blocklist {
  private static final Map<MySQLType, String> typeToMySQLMap = initialTypeMap();

  public TypeBlocklist(String typesString) {
    super(typesString);
  }

  private static HashMap<MySQLType, String> initialTypeMap() {
    HashMap<MySQLType, String> map = new HashMap<>();
    map.put(TypeDecimal, "decimal");
    map.put(TypeTiny, "tinyint");
    map.put(TypeShort, "smallint");
    map.put(TypeLong, "int");
    map.put(TypeFloat, "float");
    map.put(TypeDouble, "double");
    map.put(TypeNull, "null");
    map.put(TypeTimestamp, "timestamp");
    map.put(TypeLonglong, "bigint");
    map.put(TypeInt24, "mediumint");
    map.put(TypeDate, "date");
    map.put(TypeDuration, "time");
    map.put(TypeDatetime, "datetime");
    map.put(TypeYear, "year");
    map.put(TypeNewDate, "date");
    map.put(TypeVarchar, "varchar");
    map.put(TypeJSON, "json");
    map.put(TypeNewDecimal, "decimal");
    map.put(TypeEnum, "enum");
    map.put(TypeSet, "set");
    map.put(TypeTinyBlob, "tinytext");
    map.put(TypeMediumBlob, "mediumtext");
    map.put(TypeLongBlob, "longtext");
    map.put(TypeBlob, "text");
    map.put(TypeVarString, "varString");
    map.put(TypeString, "string");
    return map;
  }

  public boolean isUnsupportedType(MySQLType sqlType) {
    return isUnsupported(typeToMySQLMap.getOrDefault(sqlType, ""));
  }
}
