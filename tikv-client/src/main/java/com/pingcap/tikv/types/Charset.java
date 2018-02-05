/*
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
 */

package com.pingcap.tikv.types;


import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class Charset {
  // CharsetBin is used for marking binary charset.
  public static final String CharsetBin = "binary";
  // CollationBin is the default collation for CharsetBin.
  public static final String CollationBin = "binary";
  // CharsetUTF8 is the default charset for string types.
  public static final String CharsetUTF8 = "utf8";
  // CollationUTF8 is the default collation for CharsetUTF8.
  public static final String CollationUTF8 = "utf8_bin";
  // CharsetUTF8MB4 represents 4 bytes utf8, which works the same way as utf8 in Go.
  public static final String CharsetUTF8MB4 = "utf8mb4";
  // CollationUTF8MB4 is the default collation for CharsetUTF8MB4.
  public static final String CollationUTF8MB4 = "utf8mb4_bin";
  // CharsetASCII is a subset of UTF8.
  public static final String CharsetASCII = "ascii";
  // CollationASCII is the default collation for CharsetACSII.
  public static final String CollationASCII = "ascii_bin";
  // CharsetLatin1 is a single byte charset.
  public static final String CharsetLatin1 = "latin1";
  // CollationLatin1 is the default collation for CharsetLatin1.
  public static final String CollationLatin1 = "latin1_bin";

  private static final Map<String, java.nio.charset.Charset> charsetMap =
      ImmutableMap.<String, java.nio.charset.Charset>builder()
      .put("latin1", StandardCharsets.ISO_8859_1)
      .put("utf8", StandardCharsets.UTF_8)
      .put("utf8mb4", StandardCharsets.UTF_8)
      .put("ascii", StandardCharsets.US_ASCII)
      .put("binary", StandardCharsets.ISO_8859_1)
      .build();

  private static final java.nio.charset.Charset defaultCharset = StandardCharsets.UTF_8;

  public static java.nio.charset.Charset getJavaCharset(String name) {
    java.nio.charset.Charset charset = charsetMap.get(name);
    return charset != null ? charset : defaultCharset;
  }
}
