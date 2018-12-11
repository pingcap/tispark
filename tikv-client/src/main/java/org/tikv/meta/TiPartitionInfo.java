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

package org.tikv.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.List;
import shade.com.google.common.annotations.VisibleForTesting;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TiPartitionInfo implements Serializable {

  public static enum PartitionType {
    RangePartition,
    HashPartition,
    ListPartition,
  }

  private final long type;
  private final String expr;
  private final List<CIStr> columns;
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
    this.type = type;
    this.expr = expr;
    this.columns = columns;
    this.enable = enable;
    this.defs = defs;
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
}
