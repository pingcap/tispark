/*
 * Copyright 2018 PingCAP, Inc.
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

package com.pingcap.tikv.statistics;

import com.sangupta.murmur.Murmur3;
import java.util.Arrays;

public class CMSketch {
  private int depth;
  private int width;
  private long count;
  private long[][] table;

  // Hide constructor
  private CMSketch() {}

  public static CMSketch newCMSketch(int d, int w) {
    CMSketch sketch = new CMSketch();
    sketch.setTable(new long[d][w]);
    sketch.setDepth(d);
    sketch.setWidth(w);
    return sketch;
  }

  public int getDepth() {
    return depth;
  }

  public void setDepth(int depth) {
    this.depth = depth;
  }

  public int getWidth() {
    return width;
  }

  public void setWidth(int width) {
    this.width = width;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public long[][] getTable() {
    return table;
  }

  public void setTable(long[][] table) {
    this.table = table;
  }

  public long queryBytes(byte[] bytes) {
    long[] randNums = Murmur3.hash_x64_128(bytes, bytes.length, 0);
    long h1 = randNums[0];
    long h2 = randNums[1];
    long min = Long.MAX_VALUE;
    long[] vals = new long[depth];
    for (int i = 0; i < table.length; i++) {
      int j = (int) ((h1 + h2 * i) % width);
      if (min > table[i][j]) {
        min = table[i][j];
      }
      long noise = (count - table[i][j]) / (width - 1);
      if (table[i][j] < noise) {
        vals[i] = 0;
      } else {
        vals[i] = table[i][j] - noise;
      }
    }
    Arrays.sort(vals);
    long res = vals[(depth - 1) / 2] + (vals[depth / 2] - vals[(depth - 1) / 2]) / 2;
    if (res > min) {
      return min;
    }
    return res;
  }
}
