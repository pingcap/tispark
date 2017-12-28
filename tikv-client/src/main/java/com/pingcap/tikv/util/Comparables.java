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

package com.pingcap.tikv.util;

import static java.util.Objects.requireNonNull;

import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.CastingException;
import java.util.Arrays;
import java.util.Comparator;
import javax.annotation.Nonnull;

/**
 * Wrap objects into comparable types ByteString and byte[] are compared as unsigned lexicographical
 * order
 */
public class Comparables {
  public static Comparable wrap(Object o) {
    if (o == null) {
      return null;
    }
    if (o instanceof Comparable) {
      return (Comparable) o;
    }
    if (o instanceof byte[]) {
      return ComparableBytes.wrap((byte[]) o);
    }
    if (o instanceof ByteString) {
      return ComparableByteString.wrap((ByteString) o);
    }
    throw new CastingException(
        "Cannot cast to Comparable for type: " + o.getClass().getSimpleName());
  }

  public static class ComparableBytes implements Comparable<ComparableBytes> {
    // below might uses UnsafeComparator if possible
    private static final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
    private final byte[] bytes;

    private ComparableBytes(byte[] bytes) {
      requireNonNull(bytes, "bytes is null");
      this.bytes = bytes;
    }

    @Override
    public int compareTo(@Nonnull ComparableBytes other) {
      // in context of range compare and bytes compare
      // null order is not defined and causes exception
      requireNonNull(other, "other is null");
      return comparator.compare(bytes, other.bytes);
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      if (other instanceof ComparableByteString) {
        return ((ComparableBytes) other).compareTo(this) == 0;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }

    public byte[] getBytes() {
      return bytes;
    }

    public static ComparableBytes wrap(byte[] bytes) {
      return new ComparableBytes(bytes);
    }
  }

  public static class ComparableByteString implements Comparable<ComparableByteString> {
    private final ByteString bytes;

    private ComparableByteString(ByteString bytes) {
      requireNonNull(bytes, "bytes is null");
      this.bytes = bytes;
    }

    @Override
    public int compareTo(@Nonnull ComparableByteString other) {
      requireNonNull(other, "other is null");
      ByteString otherBytes = other.bytes;
      int n = Math.min(bytes.size(), otherBytes.size());
      for (int i = 0, j = 0; i < n; i++, j++) {
        int cmp = UnsignedBytes.compare(bytes.byteAt(i), otherBytes.byteAt(j));
        if (cmp != 0) return cmp;
      }
      // one is the prefix of other then the longer is larger
      return bytes.size() - otherBytes.size();
    }

    public ByteString getByteString() {
      return bytes;
    }

    public static ComparableByteString wrap(ByteString bytes) {
      return new ComparableByteString(bytes);
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      if (other instanceof ComparableByteString) {
        return ((ComparableByteString) other).compareTo(this) == 0;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return bytes.hashCode();
    }
  }
}
