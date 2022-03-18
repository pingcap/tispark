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

package com.pingcap.tikv.event;

import com.pingcap.tikv.region.TiRegion;
import java.io.Serializable;

public class CacheInvalidateEvent implements Serializable {
  private final TiRegion region;
  private final CacheType cacheType;
  private boolean invalidateRegion;
  private boolean invalidateStore;

  public CacheInvalidateEvent(
      TiRegion region, boolean updateRegion, boolean updateStore, CacheType type) {
    this.region = region;
    this.cacheType = type;
    if (updateRegion) {
      invalidateRegion();
    }

    if (updateStore) {
      invalidateStore();
    }
  }

  public TiRegion getRegion() {
    return region;
  }

  public long getStoreId() {
    return region.getLeader().getStoreId();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj instanceof CacheInvalidateEvent) {
      CacheInvalidateEvent event = (CacheInvalidateEvent) obj;
      return event.getRegion().equals(getRegion()) && event.getCacheType() == getCacheType();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = 1106;
    result += result * 31 + getRegion().hashCode();
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
    return String.format(
        "RegionId=%d,StoreId=%d,Type=%s",
        region.getId(), region.getLeader().getStoreId(), cacheType.name());
  }

  public enum CacheType implements Serializable {
    REGION_STORE,
    REQ_FAILED,
    LEADER
  }
}
