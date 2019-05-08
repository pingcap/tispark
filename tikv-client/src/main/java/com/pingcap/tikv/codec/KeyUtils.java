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

package com.pingcap.tikv.codec;

import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import org.tikv.kvproto.Coprocessor;

public class KeyUtils {

  public static String formatBytes(byte[] bytes) {
    if (bytes == null) return "null";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < bytes.length; i++) {
      int unsignedByte = UnsignedBytes.toInt(bytes[i]);
      sb.append(unsignedByte);
      if (i != bytes.length - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  public static String formatBytes(ByteString bytes) {
    if (bytes == null) return "null";
    return formatBytes(bytes.toByteArray());
  }

  public static String formatBytes(Coprocessor.KeyRange keyRange) {
    return "([" + formatBytes(keyRange.getStart()) + "], [" + formatBytes(keyRange.getEnd()) + "])";
  }

  public static String formatBytesUTF8(byte[] bytes) {
    if (bytes == null) return "null";
    return TextFormat.escapeBytes(bytes);
  }

  public static String formatBytesUTF8(ByteString bytes) {
    if (bytes == null) return "null";
    return formatBytesUTF8(bytes.toByteArray());
  }

  public static String formatBytesUTF8(Coprocessor.KeyRange keyRange) {
    return "(["
        + formatBytesUTF8(keyRange.getStart())
        + "], ["
        + formatBytesUTF8(keyRange.getEnd())
        + "])";
  }

  public static boolean hasPrefix(ByteString str, ByteString prefix) {
    if (prefix.size() > str.size()) {
      return false;
    }
    for (int i = 0; i < prefix.size(); i++) {
      if (str.byteAt(i) != prefix.byteAt(i)) {
        return false;
      }
    }
    return true;
  }
}
