package com.pingcap.tikv.meta;

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TiPartitionDef implements Serializable {
  private final long id;
  private final CIStr name;
  private final String[] lessThan;
  private final String comment;

  @JsonCreator
  @VisibleForTesting
  public TiPartitionDef(
      @JsonProperty("id") long id,
      @JsonProperty("name") CIStr name,
      @JsonProperty("less_than") String[] lessThan,
      @JsonProperty("comment") String comment) {
    this.id = id;
    this.name = name;
    this.lessThan = lessThan;
    this.comment = comment;
  }
}
