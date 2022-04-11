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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.pingcap.tikv.expression;

import static org.tikv.common.types.MySQLType.TypeBlob;
import static org.tikv.common.types.MySQLType.TypeDate;
import static org.tikv.common.types.MySQLType.TypeDatetime;
import static org.tikv.common.types.MySQLType.TypeDecimal;
import static org.tikv.common.types.MySQLType.TypeDouble;
import static org.tikv.common.types.MySQLType.TypeDuration;
import static org.tikv.common.types.MySQLType.TypeEnum;
import static org.tikv.common.types.MySQLType.TypeFloat;
import static org.tikv.common.types.MySQLType.TypeInt24;
import static org.tikv.common.types.MySQLType.TypeJSON;
import static org.tikv.common.types.MySQLType.TypeLong;
import static org.tikv.common.types.MySQLType.TypeLongBlob;
import static org.tikv.common.types.MySQLType.TypeLonglong;
import static org.tikv.common.types.MySQLType.TypeMediumBlob;
import static org.tikv.common.types.MySQLType.TypeNewDate;
import static org.tikv.common.types.MySQLType.TypeNewDecimal;
import static org.tikv.common.types.MySQLType.TypeNull;
import static org.tikv.common.types.MySQLType.TypeSet;
import static org.tikv.common.types.MySQLType.TypeShort;
import static org.tikv.common.types.MySQLType.TypeString;
import static org.tikv.common.types.MySQLType.TypeTimestamp;
import static org.tikv.common.types.MySQLType.TypeTiny;
import static org.tikv.common.types.MySQLType.TypeTinyBlob;
import static org.tikv.common.types.MySQLType.TypeVarString;
import static org.tikv.common.types.MySQLType.TypeVarchar;
import static org.tikv.common.types.MySQLType.TypeYear;

import org.tikv.common.types.MySQLType;
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
