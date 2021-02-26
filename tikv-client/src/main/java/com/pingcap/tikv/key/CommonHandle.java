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

package com.pingcap.tikv.key;

import com.pingcap.tikv.codec.Codec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.exception.CodecException;
import com.pingcap.tikv.util.FastByteComparisons;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CommonHandle implements Handle {
  private final byte[] encoded;
  private final int[] colEndOffsets;

  public CommonHandle(byte[] encoded) {
    int len = encoded.length;
    if (len < 9) {
      this.encoded = Arrays.copyOf(encoded, 9);
    } else {
      this.encoded = encoded;
    }
    int endOffset = 0;
    CodecDataInput cdi = new CodecDataInput(encoded);
    List<Integer> offsets = new ArrayList<>();
    while (!cdi.eof()) {
      if (cdi.peekByte() == 0) {
        // padded data
        break;
      }
      endOffset += cdi.cutOne();
      offsets.add(endOffset);
    }
    this.colEndOffsets = offsets.stream().mapToInt(i -> i).toArray();
  }

  public CommonHandle(byte[] encoded, int[] colEndOffsets) {
    this.encoded = encoded;
    this.colEndOffsets = colEndOffsets;
  }

  @Override
  public boolean isInt() {
    return false;
  }

  @Override
  public long intValue() {
    throw new CodecException("not supported in CommonHandle");
  }

  @Override
  public Handle next() {
    return new CommonHandle(new Key(encoded).nextPrefix().getBytes(), colEndOffsets);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof CommonHandle) {
      return Arrays.equals(encoded, ((CommonHandle) other).encoded());
    }
    return false;
  }

  @Override
  public int compare(Handle h) {
    if (h.isInt()) {
      throw new RuntimeException("CommonHandle compares to IntHandle");
    }
    return FastByteComparisons.compareTo(encoded, h.encoded());
  }

  @Override
  public byte[] encoded() {
    return this.encoded;
  }

  @Override
  public int len() {
    return this.encoded.length;
  }

  @Override
  public int numCols() {
    return this.colEndOffsets.length;
  }

  @Override
  public byte[] encodedCol(int idx) {
    int start = 0, end = colEndOffsets[idx];
    if (idx > 0) {
      start = colEndOffsets[idx - 1];
    }
    return Arrays.copyOfRange(encoded, start, end);
  }

  @Override
  public Object[] data() {
    int len = numCols();
    Object[] data = new Object[len];
    for (int i = 0; i < len; i++) {
      byte[] col = encodedCol(i);
      data[i] = Codec.decodeOne(col);
    }
    return data;
  }

  @Override
  public String toString() {
    Object[] data = data();
    return Arrays.stream(data).map(Object::toString).collect(Collectors.joining("},{", "{", "}"));
  }
}
