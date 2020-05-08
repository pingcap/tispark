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
      @JsonProperty("count") long count,
      @JsonProperty("location_labels") String[] locationLabels,
      @JsonProperty("available") boolean available,
      @JsonProperty("available_partition_ids") long[] availablePartitionIDs) {
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
