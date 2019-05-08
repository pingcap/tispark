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

import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.predicates.IndexRange;
import org.tikv.kvproto.Coprocessor.KeyRange;

// IndexScanKeyRangeBuilder accepts a table id, an index info, and an index range.
// With these info, it can build a key range which can be used for index scan.
// TODO: more refactoring on the way
public class IndexScanKeyRangeBuilder extends KeyRangeBuilder {
  private final long id;
  private final TiIndexInfo index;

  public IndexScanKeyRangeBuilder(long id, TiIndexInfo index, IndexRange ir) {
    super(ir);
    this.id = id;
    this.index = index;
  }

  private KeyRange toPairKey() {
    IndexKey lbsKey = IndexKey.toIndexKey(id, index.getId(), lPointKey, lKey);
    IndexKey ubsKey = IndexKey.toIndexKey(id, index.getId(), uPointKey, uKey);
    return makeCoprocRange(lbsKey.toByteString(), ubsKey.toByteString());
  }

  public KeyRange compute() {
    computeKeyRange();
    return toPairKey();
  }
}
