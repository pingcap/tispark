/*
 *
 * Copyright 2019 PingCAP, Inc.
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

import com.pingcap.tikv.row.Row;

public class TypeSystem {

  private static final int DEFAULT_TYPE_SYSTEM = 1;

  /**
   * Type System Version, currently support: 0 - old type system 1 - new type system, mostly
   * compatible with JDBC
   */
  private static int version = getDefaultVersion();

  private TypeSystem() {}

  private static int getDefaultVersion() {
    String defaultVersion =
        System.getProperty("com.pingcap.tikv.type_system_version", "" + DEFAULT_TYPE_SYSTEM);
    int v = Integer.parseInt(defaultVersion);
    if (!checkVersion(v)) {
      throw new RuntimeException("unsupported type system version: " + v);
    }
    System.out.println("default type system version=" + v);
    return v;
  }

  public static int getVersion() {
    return version;
  }

  public static boolean resetVersion() {
    return setVersion(DEFAULT_TYPE_SYSTEM);
  }

  public static boolean setVersion(int newVersion) {
    if (checkVersion(newVersion)) {
      version = newVersion;
      return true;
    } else {
      return false;
    }
  }

  private static boolean checkVersion(int v) {
    return v == 0 || v == 1;
  }

  public static long getFromIntType(Row row, int index) {
    if (TypeSystem.getVersion() == 1) {
      return row.getInteger(index);
    } else {
      return row.getLong(index);
    }
  }
}
