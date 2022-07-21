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

package com.pingcap.tikv;

import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.tikv.shade.com.google.common.collect.ImmutableMap;

public class Utils {
  // org.apache.spark.network.util.JavaUtils
  private static final ImmutableMap<String, TimeUnit> timeSuffixes =
      ImmutableMap.<String, TimeUnit>builder()
          .put("us", TimeUnit.MICROSECONDS)
          .put("ms", TimeUnit.MILLISECONDS)
          .put("s", TimeUnit.SECONDS)
          .put("m", TimeUnit.MINUTES)
          .put("min", TimeUnit.MINUTES)
          .put("h", TimeUnit.HOURS)
          .put("d", TimeUnit.DAYS)
          .build();

  public static ConcurrentHashMap<String, String> getSystemProperties() {
    ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
    System.getProperties()
        .stringPropertyNames()
        .forEach(key -> map.put(key, System.getProperty(key)));
    return map;
  }

  public static void setSystemProperties(ConcurrentHashMap<String, String> settings) {
    Properties prop = new Properties();
    settings.forEach(prop::setProperty);
    System.setProperties(prop);
  }

  // org.apache.spark.network.util.JavaUtils
  public static long timeStringAs(String str, TimeUnit unit) {
    String lower = str.toLowerCase(Locale.ROOT).trim();

    try {
      Matcher m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(lower);
      if (!m.matches()) {
        throw new NumberFormatException("Failed to parse time string: " + str);
      }

      long val = Long.parseLong(m.group(1));
      String suffix = m.group(2);

      // Check for invalid suffixes
      if (suffix != null && !timeSuffixes.containsKey(suffix)) {
        throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
      }

      // If suffix is valid use that, otherwise none was provided and use the default passed
      return unit.convert(val, suffix != null ? timeSuffixes.get(suffix) : unit);
    } catch (NumberFormatException e) {
      String timeError =
          "Time must be specified as seconds (s), "
              + "milliseconds (ms), microseconds (us), minutes (m or min), hour (h), or day (d). "
              + "E.g. 50s, 100ms, or 250us.";

      throw new NumberFormatException(timeError + "\n" + e.getMessage());
    }
  }

  public static long timeStringAsMs(String str) {
    return timeStringAs(str, TimeUnit.MILLISECONDS);
  }

  public static long timeStringAsSec(String str) {
    return timeStringAs(str, TimeUnit.SECONDS);
  }
}
