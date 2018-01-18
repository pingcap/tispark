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

public class TypeBlacklist extends Blacklist {
  private static final HashMap<String, String> typeToMySQLMap = initialTypeMap();

  private static HashMap<String, String> initialTypeMap() {
    HashMap<String, String> map = new HashMap<>();
    map.put("BitType", "bit");
    map.put("BytesType", "byte");
    map.put("DateTimeType", "datetime");
    map.put("TimestampType", "timestamp");
    map.put("DurationType", "time");
    map.put("DateTimeType", "datetime");
    map.put("DecimalType", "decimal");
    map.put("RealType", "double");
    map.put("IntegerType", "long");
    map.put("StringType", "string");
    map.put("EnumType", "enum");
    map.put("SetType", "set");
    return map;
  }

  public TypeBlacklist(String typesString) {
    super(typesString);
  }

  public boolean isUnsupportedType(String typeName) {
    return isUnsupported(typeToMySQLMap.getOrDefault(typeName, ""));
  }
}