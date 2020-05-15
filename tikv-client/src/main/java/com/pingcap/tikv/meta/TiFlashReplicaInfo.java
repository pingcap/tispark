/*
 * Copyright 2020 PingCAP, Inc.
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

package com.pingcap.tikv.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class TiFlashReplicaInfo implements Serializable {
  private final long count;
  private final String[] locationLabels;
  private final boolean available;
  private final long[] availablePartitionIDs;

  @JsonCreator
  public TiFlashReplicaInfo(
      @JsonProperty("Count") long count,
      @JsonProperty("LocationLabels") String[] locationLabels,
      @JsonProperty("Available") boolean available,
      @JsonProperty("AvailablePartitionIDs") long[] availablePartitionIDs) {
    this.count = count;
    this.locationLabels = locationLabels;
    this.available = available;
    this.availablePartitionIDs = availablePartitionIDs;
  }

  public boolean isPartitionAvailable(long pid) {
    for (long id : availablePartitionIDs) {
      if (id == pid) {
        return true;
      }
    }
    return false;
  }

  public long getCount() {
    return count;
  }

  public String[] getLocationLabels() {
    return locationLabels;
  }

  public boolean isAvailable() {
    return available;
  }

  public long[] getAvailablePartitionIDs() {
    return availablePartitionIDs;
  }
}
