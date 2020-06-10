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

package com.pingcap.tikv.operation.iterator;

import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tikv.exception.TiClientInternalException;
import java.util.Iterator;
import java.util.List;

public abstract class ChunkIterator<T> implements Iterator<T> {

  private final List<Chunk> chunks;
  protected int chunkIndex;
  protected int metaIndex;
  protected int bufOffset;
  protected boolean eof;

  protected ChunkIterator(List<Chunk> chunks) {
    // Read and then advance semantics
    this.chunks = chunks;
    this.chunkIndex = 0;
    this.metaIndex = 0;
    this.bufOffset = 0;
    if (chunks.size() == 0
        || chunks.get(0).getRowsMetaCount() == 0
        || chunks.get(0).getRowsData().size() == 0) {
      eof = true;
    }
  }

  public static ChunkIterator<ByteString> getRawBytesChunkIterator(List<Chunk> chunks) {
    return new ChunkIterator<ByteString>(chunks) {
      @Override
      public ByteString next() {
        Chunk c = chunks.get(chunkIndex);
        long endOffset = c.getRowsMeta(metaIndex).getLength() + bufOffset;
        if (endOffset > Integer.MAX_VALUE) {
          throw new TiClientInternalException("Offset exceeded MAX_INT.");
        }

        ByteString result = c.getRowsData().substring(bufOffset, (int) endOffset);
        advance();
        return result;
      }
    };
  }

  public static ChunkIterator<Long> getHandleChunkIterator(List<Chunk> chunks) {
    return new ChunkIterator<Long>(chunks) {
      @Override
      public Long next() {
        Chunk c = chunks.get(chunkIndex);
        long result = c.getRowsMeta(metaIndex).getHandle();
        advance();
        return result;
      }
    };
  }

  @Override
  public boolean hasNext() {
    return !eof;
  }

  private boolean seekNextNonEmptyChunk() {
    // loop until the end of chunk list or first non empty chunk
    do {
      chunkIndex += 1;
    } while (chunkIndex < chunks.size() && chunks.get(chunkIndex).getRowsMetaCount() == 0);
    // return if remaining things left
    return chunkIndex < chunks.size();
  }

  protected void advance() {
    if (eof) {
      return;
    }
    Chunk c = chunks.get(chunkIndex);
    bufOffset += c.getRowsMeta(metaIndex++).getLength();
    if (metaIndex >= c.getRowsMetaCount()) {
      if (seekNextNonEmptyChunk()) {
        metaIndex = 0;
        bufOffset = 0;
      } else {
        eof = true;
      }
    }
  }
}
