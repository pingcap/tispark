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

package com.pingcap.tikv;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.region.TiRegion;
import java.util.concurrent.Future;

/** Readonly PD client including only reading related interface Supposed for TiDB-like use cases */
public interface ReadOnlyPDClient {
  /**
   * Get Timestamp from Placement Driver
   *
   * @return a timestamp object
   */
  TiTimestamp getTimestamp();

  /**
   * Get Region from PD by key specified
   *
   * @param key key in bytes for locating a region
   * @return the region whose startKey and endKey range covers the given key
   */
  TiRegion getRegionByKey(ByteString key);

  Future<TiRegion> getRegionByKeyAsync(ByteString key);

  /**
   * Get Region by Region Id
   *
   * @param id Region Id
   * @return the region corresponding to the given Id
   */
  TiRegion getRegionByID(long id);

  Future<TiRegion> getRegionByIDAsync(long id);

  /**
   * Get Store by StoreId
   *
   * @param storeId StoreId
   * @return the Store corresponding to the given Id
   */
  Store getStore(long storeId);

  Future<Store> getStoreAsync(long storeId);

  /**
   * Close underlining resources
   *
   */
  void close() throws InterruptedException;

  /** Get associated session * @return the session associated to client */
  TiSession getSession();
}
