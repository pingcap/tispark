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

package com.pingcap.tikv.key;

import static com.pingcap.tikv.codec.KeyUtils.formatBytes;
import static java.util.Objects.requireNonNull;

import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.FastByteComparisons;
import com.pingcap.tikv.util.LogDesensitization;
import java.util.Arrays;
import javax.annotation.Nonnull;

public class Key implements Comparable<Key> {
  public static final Key EMPTY = createEmpty();
  public static final Key NULL = createNull();
  public static final Key MIN = createTypelessMin();
  public static final Key MAX = createTypelessMax();
  protected static final byte[] TBL_PREFIX = new byte[] {'t'};
  protected final byte[] value;
  protected final int infFlag;

  private Key(byte[] value, boolean negative) {
    this.value = requireNonNull(value, "value is null");
    this.infFlag = (value.length == 0 ? 1 : 0) * (negative ? -1 : 1);
  }

  protected Key(byte[] value) {
    this(value, false);
  }

  public static Key toRawKey(ByteString bytes, boolean negative) {
    return new Key(bytes.toByteArray(), negative);
  }

  public static Key toRawKey(ByteString bytes) {
    return new Key(bytes.toByteArray());
  }

  public static Key toRawKey(byte[] bytes, boolean negative) {
    return new Key(bytes, negative);
  }

  public static Key toRawKey(byte[] bytes) {
    return new Key(bytes);
  }

  private static Key createNull() {
    CodecDataOutput cdo = new CodecDataOutput();
    DataType.encodeNull(cdo);
    return new Key(cdo.toBytes()) {
      @Override
      public String toString() {
        return "null";
      }
    };
  }

  private static Key createEmpty() {
    return new Key(new byte[0]) {
      @Override
      public Key next() {
        return this;
      }

      @Override
      public String toString() {
        return "EMPTY";
      }
    };
  }

  private static Key createTypelessMin() {
    CodecDataOutput cdo = new CodecDataOutput();
    DataType.encodeIndex(cdo);
    return new Key(cdo.toBytes()) {
      @Override
      public String toString() {
        return "MIN";
      }
    };
  }

  private static Key createTypelessMax() {
    CodecDataOutput cdo = new CodecDataOutput();
    DataType.encodeMaxValue(cdo);
    return new Key(cdo.toBytes()) {
      @Override
      public String toString() {
        return "MAX";
      }
    };
  }

  /**
   * The prefixNext key for bytes domain
   *
   * <p>It first plus one at LSB and if LSB overflows, a zero byte is appended at the end Original
   * bytes will be reused if possible
   *
   * @return encoded results
   */
  static byte[] prefixNext(byte[] value) {
    int i;
    byte[] newVal = Arrays.copyOf(value, value.length);
    for (i = newVal.length - 1; i >= 0; i--) {
      newVal[i]++;
      if (newVal[i] != 0) {
        break;
      }
    }
    if (i == -1) {
      return Arrays.copyOf(value, value.length + 1);
    } else {
      return newVal;
    }
  }

  /**
   * Next key simply append a zero byte to previous key.
   *
   * @return next key with a zero byte appended
   */
  public Key next() {
    return toRawKey(Arrays.copyOf(value, value.length + 1));
  }

  /**
   * nextPrefix key will be key with next available rid. For example, if the current key is
   * prefix_rid, after calling this method, the return value should be prefix_rid+1
   *
   * @return a new key current rid+1.
   */
  public Key nextPrefix() {
    return toRawKey(prefixNext(value));
  }

  @Override
  public int compareTo(@Nonnull Key other) {
    if ((this.infFlag | other.infFlag) != 0) {
      return this.infFlag - other.infFlag;
    }
    return FastByteComparisons.compareTo(value, other.value);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other instanceof Key) {
      return compareTo((Key) other) == 0;
    } else {
      return false;
    }
  }

  public Key append(Key other) {
    if (other == null) {
      return this;
    }
    return Key.toRawKey(Bytes.concat(getBytes(), other.getBytes()));
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(value);
  }

  public byte[] getBytes() {
    return value;
  }

  public ByteString toByteString() {
    return ByteString.copyFrom(value);
  }

  public int getInfFlag() {
    return infFlag;
  }

  @Override
  public String toString() {
    if (infFlag < 0) {
      return "-INF";
    } else if (infFlag > 0) {
      return "+INF";
    } else {
      return String.format("{%s}", LogDesensitization.hide(formatBytes(value)));
    }
  }
}
