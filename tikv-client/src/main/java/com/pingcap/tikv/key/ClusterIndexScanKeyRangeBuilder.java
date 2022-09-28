package com.pingcap.tikv.key;
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

import static org.tikv.common.util.KeyRangeUtils.makeCoprocRange;

import com.pingcap.tikv.predicates.IndexRange;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.shade.com.google.protobuf.ByteString;

// ClusterIndexScanKeyRangeBuilder accepts a table id and an cluster index range.
// With these info, it can build a key range which can be used for cluster index scan.
// TODO: more refactoring on the way
public class ClusterIndexScanKeyRangeBuilder extends KeyRangeBuilder {

  private final long tableId;

  public ClusterIndexScanKeyRangeBuilder(long tableId, IndexRange ir) {
    super(ir);
    this.tableId = tableId;
  }

  private KeyRange toPairKey() {
    Key lbsKey = Key.toRawKey(lPointKey.append(lKey).getBytes());
    Key ubsKey = Key.toRawKey(uPointKey.append(uKey).getBytes());
    return makeCoprocRange(
        ByteString.copyFrom(RowKey.encode(tableId, lbsKey.getBytes())),
        ByteString.copyFrom(RowKey.encode(tableId, ubsKey.getBytes())));
  }

  public KeyRange compute() {
    computeKeyRange();
    return toPairKey();
  }
}
