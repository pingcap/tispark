package com.pingcap.tikv.meta;

import com.google.common.annotations.VisibleForTesting;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TiPartitionInfo {
  public static enum PartitionType {
    RangePartition,
    HashPartition,
    ListPartition,
  }

  private final PartitionType type;
  private final String expr;
  private final CIStr[] columns;
  private final boolean enable;
  private final TiPartitionDef[] defs;

  @JsonCreator
  @VisibleForTesting
  public TiPartitionInfo(
      @JsonProperty("type") PartitionType type,
      @JsonProperty("expr") String expr,
      @JsonProperty("columns") CIStr[] columns,
      @JsonProperty("enable") boolean enable,
      @JsonProperty("definitions") TiPartitionDef[] defs) {
    this.type = type;
    this.expr = expr;
    this.columns = columns;
    this.enable = enable;
    this.defs = defs;
  }
}
