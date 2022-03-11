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

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.pingcap.tikv.predicates.IndexRange;

// An abstract class that computes key range from IndexRange
// After computing lower and upper boundaries, the builder should
// build a key range according to its own rules.
abstract class KeyRangeBuilder {

  private final IndexRange ir;
  private final Key pointKey;
  Key lPointKey;
  Key uPointKey;
  Key lKey;
  Key uKey;

  KeyRangeBuilder(IndexRange ir) {
    this.ir = ir;
    pointKey = ir.hasAccessKey() ? Key.toRawKey(ir.getAccessKey().getBytes()) : Key.EMPTY;
  }

  private void computeWithOutRange() {
    lPointKey = pointKey;
    uPointKey = pointKey.nextPrefix();

    lKey = Key.EMPTY;
    uKey = Key.EMPTY;
  }

  private void computeWithRange() {
    Range<TypedKey> range = ir.getRange();
    lPointKey = pointKey;
    uPointKey = pointKey;

    if (!range.hasLowerBound()) {
      // -INF
      lKey = Key.NULL;
    } else {
      lKey = Key.toRawKey(range.lowerEndpoint().getBytes());
      if (range.lowerBoundType().equals(BoundType.OPEN)) {
        lKey = lKey.nextPrefix();
      }
    }

    if (!range.hasUpperBound()) {
      // INF
      uKey = Key.MAX;
    } else {
      uKey = Key.toRawKey(range.upperEndpoint().getBytes());
      if (range.upperBoundType().equals(BoundType.CLOSED)) {
        uKey = uKey.nextPrefix();
      }
    }
  }

  void computeKeyRange() {
    if (!ir.hasRange()) {
      computeWithOutRange();
    } else {
      computeWithRange();
    }
  }
}
