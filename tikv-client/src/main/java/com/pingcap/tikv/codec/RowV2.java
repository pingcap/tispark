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

package com.pingcap.tikv.codec;

import com.pingcap.tikv.exception.CodecException;
import com.pingcap.tikv.exception.InvalidCodecFormatException;
import java.util.Arrays;

public class RowV2 {
  // small:  colID byte[], offsets int[], optimized for most cases.
  // large:  colID long[], offsets long[].
  boolean large;
  int numNotNullCols;
  int numNullCols;
  byte[] colIDs;

  int[] offsets;
  byte[] data;

  // for large row
  int[] colIDs32;
  int[] offsets32;

  // CodecVer is the constant number that represent the new row format.
  public static int CODEC_VER = 0x80;

  public RowV2(byte[] rowData) {
    fromBytes(rowData);
  }

  public RowV2(
      boolean large,
      int numNotNullCols,
      int numNullCols,
      byte[] colIDs,
      int[] offsets,
      byte[] data) {
    this.large = large;
    this.numNotNullCols = numNotNullCols;
    this.numNullCols = numNullCols;
    this.colIDs = colIDs;
    this.offsets = offsets;
    this.data = data;
  }

  public RowV2(
      boolean large,
      int numNotNullCols,
      int numNullCols,
      int[] colIDs32,
      int[] offsets32,
      byte[] data) {
    this.large = large;
    this.numNotNullCols = numNotNullCols;
    this.numNullCols = numNullCols;
    this.colIDs32 = colIDs32;
    this.offsets32 = offsets32;
    this.data = data;
  }

  public byte[] getData(int i) {
    int start = 0, end = 0;
    if (this.large) {
      if (i > 0) {
        start = this.offsets32[i - 1];
      }
      end = this.offsets32[i];
    } else {
      if (i > 0) {
        start = this.offsets[i - 1];
      }
      end = this.offsets[i];
    }
    return Arrays.copyOfRange(this.data, start, end);
  }

  private void fromBytes(byte[] rowData) {
    CodecDataInputLittleEndian cdi = new CodecDataInputLittleEndian(rowData);
    if (cdi.readUnsignedByte() != CODEC_VER) {
      throw new InvalidCodecFormatException("invalid codec version");
    }
    this.large = (cdi.readUnsignedByte() & 1) > 0;
    this.numNotNullCols = cdi.readUnsignedShort();
    this.numNullCols = cdi.readUnsignedShort();
    int cursor = 6;
    if (this.large) {
      int numCols = this.numNotNullCols + this.numNullCols;
      int colIDsLen = numCols * 4;
      this.colIDs32 = new int[numCols];
      for (int i = 0; i < numCols; i++) {
        this.colIDs32[i] = cdi.readInt();
      }
      cursor += colIDsLen;
      numCols = this.numNotNullCols;
      int offsetsLen = numCols * 4;
      this.offsets32 = new int[numCols];
      for (int i = 0; i < numCols; i++) {
        this.offsets32[i] = cdi.readInt();
      }
      cursor += offsetsLen;
    } else {
      int numCols = this.numNotNullCols + this.numNullCols;
      int colIDsLen = numCols;
      this.colIDs = new byte[numCols];
      cdi.readFully(this.colIDs, 0, numCols);
      cursor += colIDsLen;
      numCols = this.numNotNullCols;
      int offsetsLen = numCols * 2;
      this.offsets = new int[numCols];
      for (int i = 0; i < numCols; i++) {
        this.offsets[i] = cdi.readUnsignedShort();
      }
      cursor += offsetsLen;
    }
    this.data = Arrays.copyOfRange(rowData, cursor, rowData.length);
  }

  public byte[] toBytes() {
    throw new CodecException("not implemented yet");
  }

  public static class ColIDSearchResult {
    int idx;
    boolean isNull;
    boolean notFound;

    private ColIDSearchResult(int idx, boolean isNull, boolean notFound) {
      this.idx = idx;
      this.isNull = isNull;
      this.notFound = notFound;
    }
  }

  private int binarySearch(int i, int j, long colID) {
    while (i < j) {
      int h = (int) ((i + (long) j) >> 1);
      // i <= h < j
      long v;
      if (this.large) {
        v = this.colIDs32[h];
      } else {
        v = this.colIDs[h];
      }
      if (v < colID) {
        i = h + 1;
      } else if (v > colID) {
        j = h;
      } else {
        return h;
      }
    }
    return -1;
  }

  public ColIDSearchResult findColID(long colID) {
    int i = 0, j = this.numNotNullCols;
    ColIDSearchResult result = new ColIDSearchResult(-1, false, false);
    result.idx = binarySearch(i, j, colID);
    if (result.idx != -1) {
      return result;
    }

    // Search the column in null columns array.
    i = this.numNotNullCols;
    j = this.numNotNullCols + this.numNullCols;
    int id = binarySearch(i, j, colID);
    if (id != -1) {
      // colID found in null cols.
      result.isNull = true;
    } else {
      result.notFound = true;
    }
    return result;
  }

  public void initColIDs() {
    int numCols = this.numNotNullCols + this.numNullCols;
    this.colIDs = new byte[numCols];
  }

  public void initColIDs32() {
    int numCols = this.numNotNullCols + this.numNullCols;
    this.colIDs32 = new int[numCols];
  }

  public void initOffsets() {
    this.offsets = new int[this.numNotNullCols];
  }

  public void initOffsets32() {
    this.offsets32 = new int[this.numNotNullCols];
  }
}
