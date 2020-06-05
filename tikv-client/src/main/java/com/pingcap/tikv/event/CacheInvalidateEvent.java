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

package com.pingcap.tikv.event;

import java.io.Serializable;

public class CacheInvalidateEvent implements Serializable {
  private final long regionId;
  private final long storeId;
  private final CacheType cacheType;
  private boolean invalidateRegion;
  private boolean invalidateStore;

  public CacheInvalidateEvent(
      long regionId, long storeId, boolean updateRegion, boolean updateStore, CacheType type) {
    this.regionId = regionId;
    this.storeId = storeId;
    this.cacheType = type;
    if (updateRegion) {
      invalidateRegion();
    }

    if (updateStore) {
      invalidateStore();
    }
  }

  public long getRegionId() {
    return regionId;
  }

  public long getStoreId() {
    return storeId;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj instanceof CacheInvalidateEvent) {
      CacheInvalidateEvent event = (CacheInvalidateEvent) obj;
      return event.getRegionId() == getRegionId()
          && event.getStoreId() == getStoreId()
          && event.getCacheType() == getCacheType();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = 1106;
    result += result * 31 + getStoreId();
    result += result * 31 + getRegionId();
    result += result * 31 + getCacheType().name().hashCode();
    return result;
  }

  public void invalidateRegion() {
    invalidateRegion = true;
  }

  public void invalidateStore() {
    invalidateStore = true;
  }

  public boolean shouldUpdateRegion() {
    return invalidateRegion;
  }

  public boolean shouldUpdateStore() {
    return invalidateStore;
  }

  public CacheType getCacheType() {
    return cacheType;
  }

  @Override
  public String toString() {
    return String.format("RegionId=%d,StoreId=%d,Type=%s", regionId, storeId, cacheType.name());
  }

  public enum CacheType implements Serializable {
    REGION_STORE,
    REQ_FAILED,
    LEADER
  }
}
