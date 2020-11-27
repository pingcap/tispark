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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.util;

public class LogDesensitization {
  private static boolean enableLogDesensitization = getLogDesensitization();

  public static String hide(String info) {
    if (enableLogDesensitization) {
      return "*";
    } else {
      return info;
    }
  }

  /**
   * TiSparkLogDesensitizationLevel = 1 => disable LogDesensitization, otherwise enable
   * LogDesensitization
   *
   * @return true enable LogDesensitization, false disable LogDesensitization
   */
  private static boolean getLogDesensitization() {
    String tiSparkLogDesensitizationLevel = "TiSparkLogDesensitizationLevel";
    String tmp = System.getenv(tiSparkLogDesensitizationLevel);
    if (tmp != null && !"".equals(tmp)) {
      return !"1".equals(tmp);
    }

    tmp = System.getProperty(tiSparkLogDesensitizationLevel);
    if (tmp != null && !"".equals(tmp)) {
      return !"1".equals(tmp);
    }

    return true;
  }
}
