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

import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.CodecException;

public class IntHandle implements Handle {
  private final long handle;

  public IntHandle(long handle) {
    this.handle = handle;
  }

  @Override
  public boolean isInt() {
    return true;
  }

  @Override
  public long intValue() {
    return handle;
  }

  @Override
  public Handle next() {
    return new IntHandle(handle + 1);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof IntHandle) {
      return ((IntHandle) other).intValue() == handle;
    }
    return false;
  }

  @Override
  public int compare(Handle h) {
    if (!h.isInt()) {
      throw new RuntimeException("IntHandle compares to CommonHandle");
    }
    long val = intValue();
    long hVal = h.intValue();
    if (val > hVal) {
      return 1;
    } else if (val < hVal) {
      return -1;
    }
    return 0;
  }

  @Override
  public byte[] encoded() {
    CodecDataOutput cdo = new CodecDataOutput();
    IntegerCodec.writeLong(cdo, handle);
    return cdo.toBytes();
  }

  @Override
  public int len() {
    return 8;
  }

  @Override
  public int numCols() {
    throw new CodecException("not supported in IntHandle");
  }

  @Override
  public byte[] encodedCol(int idx) {
    throw new CodecException("not supported in IntHandle");
  }

  @Override
  public Object[] data() {
    return new Object[] {handle};
  }

  @Override
  public String toString() {
    return String.valueOf(handle);
  }
}
