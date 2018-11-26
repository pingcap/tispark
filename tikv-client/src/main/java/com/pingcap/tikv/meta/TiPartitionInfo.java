/*
 * Copyright 2018 PingCAP, Inc.
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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TiPartitionInfo implements Serializable {

  public enum PartitionType implements Serializable {
    RangePartition,
    HashPartition,
    ListPartition,
  }

  private final PartitionType type;
  private final String expr;
  private final List<CIStr> columns;
  private final boolean enable;
  private List<TiPartitionDef> defs;

  @VisibleForTesting
  @JsonCreator
  public TiPartitionInfo(
      @JsonProperty("type") long type,
      @JsonProperty("expr") String expr,
      @JsonProperty("columns") List<CIStr> columns,
      @JsonProperty("enable") boolean enable,
      @JsonProperty("definitions") List<TiPartitionDef> defs) {
    this.type = toPartTp(type);
    this.expr = expr;
    this.columns = columns;
    this.enable = enable;
    this.defs = defs;
  }

  private PartitionType toPartTp(long type) {
    if (type == 1L) {
      return PartitionType.RangePartition;
    } else if (type == 2L) {
      return PartitionType.HashPartition;
    } else if (type == 3L) {
      return PartitionType.ListPartition;
    }
    // only allow range, hash, list partition.
    throw new IllegalArgumentException(String.format("Partition type %d is invalid", type));
  }

  private long PartTpToLong(PartitionType partTp) {
    switch (partTp) {
      case RangePartition:
        return 1;
      case HashPartition:
        return 2;
      case ListPartition:
        return 3;
    }
    return 0;
  }

  public PartitionType getType() {
    return this.type;
  }

  public boolean isEnable() {
    return enable;
  }

  public List<CIStr> getColumns() {
    return columns;
  }

  public String getExpr() {
    return expr;
  }

  public List<TiPartitionDef> getDefs() {
    return defs;
  }

  public void setDefs(List<TiPartitionDef> defs) {
    this.defs = defs;
  }

  @Override
  public TiPartitionInfo clone() {
    List<CIStr> newCols = new ArrayList<>();
    if (columns != null) {
      columns.forEach((col) -> newCols.add(col.clone()));
    }
    List<TiPartitionDef> partDefs = new ArrayList<>();
    if (defs != null) {
      defs.forEach((def) -> partDefs.add(def.clone()));
    }
    TiPartitionInfo partInfo =
        new TiPartitionInfo(PartTpToLong(this.type), this.expr, newCols, this.enable, partDefs);
    return partInfo;
  }
}
