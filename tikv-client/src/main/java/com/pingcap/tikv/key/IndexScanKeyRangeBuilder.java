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

import static com.pingcap.tikv.util.KeyRangeUtils.makeCoprocRange;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.predicates.IndexRange;
import org.tikv.kvproto.Coprocessor.KeyRange;

// IndexScanKeyRangeBuilder accepts a table id, a index info, and a index range.
// With these info, it can build a keyrange which can be used for index scan.
// TODO: more refactoring on the way
public class IndexScanKeyRangeBuilder {
  private final long id;
  private final TiIndexInfo index;
  private final IndexRange ir;
  private final Key pointKey;
  private Key lPointKey;
  private Key uPointKey;
  private Key lKey;
  private Key uKey;

  public IndexScanKeyRangeBuilder(long id, TiIndexInfo index, IndexRange ir) {
    this.id = id;
    this.index = index;
    this.ir = ir;
    pointKey = ir.hasAccessKey() ? ir.getAccessKey() : Key.EMPTY;
  }

  private KeyRange computeWithOutRange() {
    lPointKey = pointKey;
    uPointKey = pointKey.nextPrefix();

    lKey = Key.EMPTY;
    uKey = Key.EMPTY;
    return toPairKey();
  }

  private KeyRange toPairKey() {
    IndexKey lbsKey = IndexKey.toIndexKey(id, index.getId(), lPointKey, lKey);
    IndexKey ubsKey = IndexKey.toIndexKey(id, index.getId(), uPointKey, uKey);
    return makeCoprocRange(lbsKey.toByteString(), ubsKey.toByteString());
  }

  private KeyRange computeWithRange() {
    Range<TypedKey> range = ir.getRange();
    lPointKey = pointKey;
    uPointKey = pointKey;

    if (!range.hasLowerBound()) {
      // -INF
      lKey = Key.NULL;
    } else {
      lKey = range.lowerEndpoint();
      if (range.lowerBoundType().equals(BoundType.OPEN)) {
        lKey = lKey.nextPrefix();
      }
    }

    if (!range.hasUpperBound()) {
      // INF
      uKey = Key.MAX;
    } else {
      uKey = range.upperEndpoint();
      if (range.upperBoundType().equals(BoundType.CLOSED)) {
        uKey = uKey.nextPrefix();
      }
    }
    return toPairKey();
  }

  public KeyRange compute() {
    if (!ir.hasRange()) {
      return computeWithOutRange();
    } else {
      return computeWithRange();
    }
  }
}
