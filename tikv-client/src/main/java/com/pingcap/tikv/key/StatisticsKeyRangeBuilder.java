/*
 * Copyright 2019 PingCAP, Inc.
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

import com.pingcap.tikv.predicates.IndexRange;
import com.pingcap.tikv.util.Pair;

// A builder to build key range for Statistics keys
public class StatisticsKeyRangeBuilder extends KeyRangeBuilder {

  public StatisticsKeyRangeBuilder(IndexRange ir) {
    super(ir);
  }

  private Pair<Key, Key> toPairKey() {
    Key lbsKey = Key.toRawKey(lPointKey.append(lKey).getBytes());
    Key ubsKey = Key.toRawKey(uPointKey.append(uKey).getBytes());
    return new Pair<>(lbsKey, ubsKey);
  }

  public Pair<Key, Key> compute() {
    computeKeyRange();
    return toPairKey();
  }
}
