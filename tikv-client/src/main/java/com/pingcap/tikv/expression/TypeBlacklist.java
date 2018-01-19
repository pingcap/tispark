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

import java.util.HashMap;
import java.util.Map;

public class TypeBlacklist extends Blacklist {
  private static final Map<String, String> typeToMySQLMap = initialTypeMap();

  private static HashMap<String, String> initialTypeMap() {
    HashMap<String, String> map = new HashMap<>();
    map.put("TypeDecimal", "decimal");
    map.put("TypeTiny", "tinyint");
    map.put("TypeShort", "smallint");
    map.put("TypeLong", "int");
    map.put("TypeFloat", "float");
    map.put("TypeDouble", "double");
    map.put("TypeNull", "null");
    map.put("TypeTimestamp", "timestamp");
    map.put("TypeLonglong", "bigint");
    map.put("TypeInt24", "mediumint");
    map.put("TypeDate", "date");
    map.put("TypeDuration", "time");
    map.put("TypeDatetime", "datetime");
    map.put("TypeYear", "year");
    map.put("TypeNewDate", "date");
    map.put("TypeVarchar", "varchar");
    map.put("TypeJSON", "json");
    map.put("TypeNewDecimal", "decimal");
    map.put("TypeEnum", "enum");
    map.put("TypeSet", "set");
    map.put("TypeTinyBlob", "tinytext");
    map.put("TypeMediumBlob", "mediumtext");
    map.put("TypeLongBlob", "longtext");
    map.put("TypeBlob", "text");
    map.put("TypeVarString", "varstring");
    map.put("TypeString", "string");
    return map;
  }

  public TypeBlacklist(String typesString) {
    super(typesString);
  }

  public boolean isUnsupportedType(String typeName) {
    return isUnsupported(typeToMySQLMap.getOrDefault(typeName, ""));
  }
}