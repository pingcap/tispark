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

public class BinPaddingCollator {

  // if a > b return >1, if a < b return -1, if a == b return 0
  public static int compare(String a, String b) {
    int res = Collation.truncateTailingSpace(a).compareTo(Collation.truncateTailingSpace(b));
    return (res > 0) ? 1 : (res < 0) ? -1 : 0;
  }

  // truncate tail space and encode string to bytes
  public static byte[] key(String key) {
    return Collation.truncateTailingSpace(key).getBytes();
  }

  // encode string to bytes
  public static byte[] keyWithoutTrimRightSpace(String key) {
    return key.getBytes();
  }
}
