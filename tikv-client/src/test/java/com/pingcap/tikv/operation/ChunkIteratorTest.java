/*
 *
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
 *
 */

package com.pingcap.tikv.operation;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.RowMeta;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.operation.iterator.ChunkIterator;
import com.pingcap.tikv.row.ObjectRowImpl;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.StringType;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class ChunkIteratorTest {
  private final List<Chunk> chunks = new ArrayList<>();

  private static void setValueToRow(CodecDataInput cdi, DataType type, int pos, Row row) {
    if (type.isNextNull(cdi)) {
      cdi.readUnsignedByte();
      row.setNull(pos);
    } else {
      row.set(pos, type, type.decode(cdi));
    }
  }

  @Before
  public void setup() {
    // 8 2 2 2 a 8 4 2 2 b 8 6 2 2 c
    // 1 a 2 b 3 c
    String chunkStr = "\b\u0002\u0002\u0002a\b\u0004\u0002\u0002b\b\u0006\u0002\u0002c";
    Chunk chunk =
        Chunk.newBuilder()
            .setRowsData(ByteString.copyFromUtf8(chunkStr))
            .addRowsMeta(0, RowMeta.newBuilder().setHandle(1).setLength(5))
            .addRowsMeta(1, RowMeta.newBuilder().setHandle(2).setLength(5))
            .addRowsMeta(2, RowMeta.newBuilder().setHandle(3).setLength(5))
            .build();
    chunks.add(chunk);
  }

  @Test
  public void chunkTest() {
    ChunkIterator<ByteString> chunkIterator = ChunkIterator.getRawBytesChunkIterator(chunks);
    DataType bytes = StringType.VARCHAR;
    DataType ints = IntegerType.INT;
    Row row = ObjectRowImpl.create(6);
    CodecDataInput cdi = new CodecDataInput(chunkIterator.next());
    setValueToRow(cdi, ints, 0, row);
    setValueToRow(cdi, bytes, 1, row);
    cdi = new CodecDataInput(chunkIterator.next());
    setValueToRow(cdi, ints, 2, row);
    setValueToRow(cdi, bytes, 3, row);
    cdi = new CodecDataInput(chunkIterator.next());
    setValueToRow(cdi, ints, 4, row);
    setValueToRow(cdi, bytes, 5, row);
    assertEquals(row.getLong(0), 1);
    assertEquals(row.getString(1), "a");
    assertEquals(row.getLong(2), 2);
    assertEquals(row.getString(3), "b");
    assertEquals(row.getLong(4), 3);
    assertEquals(row.getString(5), "c");
  }
}
