/*
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
 */

package com.pingcap.tikv.txn.type;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.Pair;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.tikv.kvproto.Metapb;

public class GroupKeyResult {

  private Map<Pair<TiRegion, Metapb.Store>, List<ByteString>> groupsResult;

  public GroupKeyResult() {
    this.groupsResult = new HashMap<>();
  }

  public Map<Pair<TiRegion, Metapb.Store>, List<ByteString>> getGroupsResult() {
    return groupsResult;
  }

  public void setGroupsResult(Map<Pair<TiRegion, Metapb.Store>, List<ByteString>> groupsResult) {
    this.groupsResult = groupsResult;
  }
}
