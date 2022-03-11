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

  private final long type;
  private final String expr;
  private final List<String> columns;
  private final boolean enable;
  private final List<TiPartitionDef> defs;

  @VisibleForTesting
  @JsonCreator
  public TiPartitionInfo(
      @JsonProperty("type") long type,
      @JsonProperty("expr") String expr,
      @JsonProperty("columns") List<CIStr> columns,
      @JsonProperty("enable") boolean enable,
      @JsonProperty("definitions") List<TiPartitionDef> defs) {
    // Part type only contains limited amount, so long to int conversion
    // should be safe.
    this.type = type;
    this.expr = expr;
    this.columns = new ArrayList<>();

    // range column or list column case
    if (columns != null) {
      for (CIStr col : columns) {
        this.columns.add(col.getL());
      }
    }
    this.enable = enable;
    this.defs = defs;
  }

  public static long partTypeToLong(PartitionType type) {
    switch (type) {
      case RangePartition:
        return 1L;
      case HashPartition:
        return 2L;
      case ListPartition:
        return 3L;
    }
    return -1;
  }

  public PartitionType getType() {
    return toPartType((int) type);
  }

  private PartitionType toPartType(int tp) {
    switch (tp) {
      case 1:
        return PartitionType.RangePartition;
      case 2:
        return PartitionType.HashPartition;
      case 3:
        return PartitionType.ListPartition;
    }
    return PartitionType.InvalidPartition;
  }

  public boolean isEnable() {
    return enable;
  }

  public List<String> getColumns() {
    return columns;
  }

  public String getExpr() {
    return expr;
  }

  public List<TiPartitionDef> getDefs() {
    return defs;
  }

  public enum PartitionType {
    RangePartition,
    HashPartition,
    ListPartition,
    InvalidPartition,
  }
}
