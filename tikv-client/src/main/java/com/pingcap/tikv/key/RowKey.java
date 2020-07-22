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

import static com.pingcap.tikv.codec.Codec.IntegerCodec.writeLong;

import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.exception.TiExpressionException;
import java.io.Serializable;

public class RowKey extends Key implements Serializable {
  private static final byte[] REC_PREFIX_SEP = new byte[] {'_', 'r'};

  private final long tableId;
  private final long handle;
  private final boolean maxHandleFlag;

  private RowKey(long tableId, long handle) {
    super(encode(tableId, handle));
    this.tableId = tableId;
    this.handle = handle;
    this.maxHandleFlag = false;
  }

  /**
   * The RowKey indicating maximum handle (its value exceeds Long.Max_Value)
   *
   * <p>Initializes an imaginary globally MAXIMUM rowKey with tableId.
   */
  private RowKey(long tableId) {
    super(encodeBeyondMaxHandle(tableId));
    this.tableId = tableId;
    this.handle = Long.MAX_VALUE;
    this.maxHandleFlag = true;
  }

  public static RowKey toRowKey(long tableId, long handle) {
    return new RowKey(tableId, handle);
  }

  public static RowKey toRowKey(long tableId, TypedKey handle) {
    Object obj = handle.getValue();
    if (obj instanceof Long) {
      return new RowKey(tableId, (long) obj);
    }
    throw new TiExpressionException("Cannot encode row key with non-long type");
  }

  public static RowKey createMin(long tableId) {
    return toRowKey(tableId, Long.MIN_VALUE);
  }

  public static RowKey createBeyondMax(long tableId) {
    return new RowKey(tableId);
  }

  public static RowKey decode(byte[] value) {
    CodecDataInput cdi = new CodecDataInput(value);
    cdi.readByte();
    long tableId = IntegerCodec.readLong(cdi); // tableId
    cdi.readByte();
    cdi.readByte();
    long handle = IntegerCodec.readLong(cdi); // handle
    return toRowKey(tableId, handle);
  }

  private static byte[] encode(long tableId, long handle) {
    CodecDataOutput cdo = new CodecDataOutput();
    encodePrefix(cdo, tableId);
    writeLong(cdo, handle);
    return cdo.toBytes();
  }

  private static byte[] encodeBeyondMaxHandle(long tableId) {
    return prefixNext(encode(tableId, Long.MAX_VALUE));
  }

  private static void encodePrefix(CodecDataOutput cdo, long tableId) {
    cdo.write(TBL_PREFIX);
    writeLong(cdo, tableId);
    cdo.write(REC_PREFIX_SEP);
  }

  @Override
  public RowKey next() {
    long handle = getHandle();
    boolean maxHandleFlag = getMaxHandleFlag();
    if (maxHandleFlag) {
      throw new TiClientInternalException("Handle overflow for Long MAX");
    }
    if (handle == Long.MAX_VALUE) {
      return createBeyondMax(tableId);
    }
    return new RowKey(tableId, handle + 1);
  }

  public long getTableId() {
    return tableId;
  }

  public long getHandle() {
    return handle;
  }

  private boolean getMaxHandleFlag() {
    return maxHandleFlag;
  }

  @Override
  public String toString() {
    return Long.toString(handle);
  }

  public static class DecodeResult {
    public long handle;
    public Status status;

    public enum Status {
      MIN,
      MAX,
      EQUAL,
      LESS,
      GREATER,
      UNKNOWN_INF
    }
  }
}
