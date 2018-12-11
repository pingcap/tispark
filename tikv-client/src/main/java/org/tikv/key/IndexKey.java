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

package org.tikv.key;

import org.tikv.codec.Codec.IntegerCodec;
import org.tikv.codec.CodecDataOutput;
import org.tikv.exception.TypeException;
import shade.com.google.common.base.Joiner;

public class IndexKey extends Key {
  private static final byte[] IDX_PREFIX_SEP = new byte[] {'_', 'i'};

  private final long tableId;
  private final long indexId;
  private final Key[] dataKeys;

  private IndexKey(long tableId, long indexId, Key[] dataKeys) {
    super(encode(tableId, indexId, dataKeys));
    this.tableId = tableId;
    this.indexId = indexId;
    this.dataKeys = dataKeys;
  }

  public static IndexKey toIndexKey(long tableId, long indexId, Key... dataKeys) {
    return new IndexKey(tableId, indexId, dataKeys);
  }

  private static byte[] encode(long tableId, long indexId, Key[] dataKeys) {
    CodecDataOutput cdo = new CodecDataOutput();
    cdo.write(TBL_PREFIX);
    IntegerCodec.writeLong(cdo, tableId);
    cdo.write(IDX_PREFIX_SEP);
    IntegerCodec.writeLong(cdo, indexId);
    for (Key key : dataKeys) {
      if (key == null) {
        throw new TypeException("key cannot be null");
      }
      cdo.write(key.getBytes());
    }
    return cdo.toBytes();
  }

  public long getTableId() {
    return tableId;
  }

  public long getIndexId() {
    return indexId;
  }

  public Key[] getDataKeys() {
    return dataKeys;
  }

  @Override
  public String toString() {
    return String.format("[%s]", Joiner.on(",").useForNull("null").join(dataKeys));
  }
}
