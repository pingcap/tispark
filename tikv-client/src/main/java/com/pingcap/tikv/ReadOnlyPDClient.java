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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv;

import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.Pair;
import java.util.concurrent.Future;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.shade.com.google.protobuf.ByteString;

/** Readonly PD client including only reading related interface Supposed for TiDB-like use cases */
public interface ReadOnlyPDClient extends org.tikv.common.ReadOnlyPDClient {

  Future<Pair<Metapb.Region, Metapb.Peer>> getRegionByKeyAsync(BackOffer backOffer, ByteString key);

  Future<Pair<Metapb.Region, Metapb.Peer>> getRegionByIDAsync(BackOffer backOffer, long id);

  Future<Store> getStoreAsync(BackOffer backOffer, long storeId);
}
