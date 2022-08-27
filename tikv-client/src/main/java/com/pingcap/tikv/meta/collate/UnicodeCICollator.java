/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.meta.collate;

import com.pingcap.tikv.meta.Collation;
import com.pingcap.tikv.util.Pair;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnicodeCICollator {
  private static final Logger logger = LoggerFactory.getLogger(UnicodeCICollator.class);

  // if a > b return 1, if a < b return -1, if a == b return 0
  public static int compare(String a, String b) {
    a = Collation.truncateTailingSpace(a);
    b = Collation.truncateTailingSpace(b);
    long an = 0, bn = 0;
    long as = 0, bs = 0;
    int ar = 0, br = 0;
    int ai = 0, bi = 0;

    while (true) {
      if (an == 0) {
        if (as == 0) {
          while (an == 0 && ai < a.length()) {
            Pair<Integer, Integer> pair = Collation.decodeRune(a, ai);
            ar = pair.first;
            ai = pair.second;
            Pair<Long, Long> pairL = convertRuneUnicodeCI(ar);
            an = pairL.first;
            as = pairL.second;
          }
        } else {
          an = as;
          as = 0;
        }
      }
      if (bn == 0) {
        if (bs == 0) {
          while (bn == 0 && bi < b.length()) {
            Pair<Integer, Integer> pair = Collation.decodeRune(b, bi);
            br = pair.first;
            bi = pair.second;
            Pair<Long, Long> pairL = convertRuneUnicodeCI(br);
            bn = pairL.first;
            bs = pairL.second;
          }
        } else {
          bn = bs;
          bs = 0;
        }
      }

      if (an == 0 || bn == 0) {
        return Collation.sign((int) an - (int) bn);
      }

      if (an == bn) {
        an = 0;
        bn = 0;
        continue;
      }

      while (an != 0 && bn != 0) {
        if (((an ^ bn) & 0xFFFF) != 0) {
          return Collation.sign((int) (an & 0xFFFF) - (int) (bn & 0xFFFF));
        }
        an >>= 16;
        bn >>= 16;
      }
    }
  }

  // truncate tail space and encode string to bytes
  public static byte[] key(String key) {
    return keyWithoutTrimRightSpace(Collation.truncateTailingSpace(key));
  }

  public static byte[] keyWithoutTrimRightSpace(String key) {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    int r = 0, si = 0;
    long sn = 0, ss = 0;
    while (si < key.getBytes().length) {
      Pair<Integer, Integer> pair = Collation.decodeRune(key, si);
      r = pair.first;
      si = pair.second;
      Pair<Long, Long> pair2 = convertRuneUnicodeCI(r);
      sn = pair2.first;
      ss = pair2.second;
      while (sn != 0) {
        buf.write((byte) ((sn & 0xFF00) >> 8));
        buf.write((byte) (sn & 0xFF));
        sn = sn >> 16;
      }
      while (ss != 0) {
        buf.write((byte) ((ss & 0xFF00) >> 8));
        buf.write((byte) (ss & 0xFF));
        ss = ss >> 16;
      }
    }
    return buf.toByteArray();
  }

  public static Pair<Long, Long> convertRuneUnicodeCI(int r) {
    if (r > 0xFFFF) {
      return new Pair<>(0xFFFDL, 0L);
    }
    if (mapTable[r] == 0xFFFDL) {
      return new Pair<>(longRuneMap.get(r)[0], longRuneMap.get(r)[1]);
    }
    return new Pair<>(mapTable[r], 0L);
  }

  private static final String UNICODE_MAP_TABLE = "unicode_map_table";
  private static final long[] mapTable;
  // This step cost about 10 seconds.
  static {
    try {
      Long startTime = System.currentTimeMillis();
      InputStream inputStream =
          UnicodeCICollator.class.getClassLoader().getResourceAsStream(UNICODE_MAP_TABLE);
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
      List<String> lines = new ArrayList<>();
      for (String line; (line = reader.readLine()) != null; ) {
        lines.add(line);
      }
      logger.info(
          "Read unicode_map_table file cost " + (System.currentTimeMillis() - startTime) + "ms");
      startTime = System.currentTimeMillis();
      mapTable =
          lines
              .stream()
              .map(line -> line.split(","))
              .flatMap(Arrays::stream)
              .map(
                  s -> {
                    if (s.startsWith("0x")) {
                      return s.substring(2);
                    } else {
                      return s;
                    }
                  })
              .mapToLong(l -> Long.parseUnsignedLong(l, 16))
              .toArray();
      logger.info("Load unicode_map_table cost " + (System.currentTimeMillis() - startTime) + "ms");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final Map<Integer, long[]> longRuneMap =
      Arrays.stream(
              new Object[][] {
                {0x321D, new long[] {0x1D6E1DC61D6D0288L, 0x2891E031DC2L}},
                {0x321E, new long[] {0x1D741DC61D6D0288L, 0x2891DCBL}},
                {0x327C, new long[] {0x1D621E0F1DBE1D70L, 0x1DC6}},
                {0x3307, new long[] {0xE0B1E591E5E1E55L, 0x1E65}},
                {0x3315, new long[] {0x1E781E591E7C1E58L, 0x1E72}},
                {0x3316, new long[] {0xE0B1E731E7C1E58L, 0x1E7A1E65}},
                {0x3317, new long[] {0x1E631E7D1E7C1E58L, 0x1E65}},
                {0x3319, new long[] {0x1E651E721E781E59L, 0x1E81}},
                {0x331A, new long[] {0x1E531E5F1E7A1E59L, 0x1E7C}},
                {0x3320, new long[] {0xE0B1E621E811E5CL, 0x1E72}},
                {0x332B, new long[] {0x1E811E5F0E0B1E6BL, 0x1E65}},
                {0x332E, new long[] {0x1E651E5E1E521E6CL, 0x1E7A}},
                {0x3332, new long[] {0x1E631E781E521E6DL, 0x1E65}},
                {0x3334, new long[] {0x1E551E5D1E631E6DL, 0x1E7A}},
                {0x3336, new long[] {0xE0B1E611E591E6EL, 0x1E7A}},
                {0x3347, new long[] {0x1E771E5D1E811E70L, 0x1E81}},
                {0x334A, new long[] {0xE0B1E6B1E791E71L, 0x1E7A}},
                {0x3356, new long[] {0x1E5A1E651E811E7BL, 0x1E81}},
                {0x337F, new long[] {0xDF0FFB40E82AFB40L, 0xF93EFB40CF1AFB40L}},
                {0x33AE, new long[] {0x4370E6D0E330FC0L, 0xFEA}},
                {0x33AF, new long[] {0x4370E6D0E330FC0L, 0xE2B0FEA}},
                {0xFDFB, new long[] {0x135E020913AB135EL, 0x13B713AB135013ABL}}
              })
          .collect(Collectors.toMap(o -> (Integer) o[0], o -> (long[]) o[1]));
}
