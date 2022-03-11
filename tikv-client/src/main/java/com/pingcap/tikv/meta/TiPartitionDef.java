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
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TiPartitionDef implements Serializable {
  private final long id;
  private final String name;
  private final List<String> lessThan;
  private final String comment;

  @VisibleForTesting
  @JsonCreator
  public TiPartitionDef(
      @JsonProperty("id") long id,
      @JsonProperty("name") CIStr name,
      @JsonProperty("less_than") List<String> lessThan,
      @JsonProperty("comment") String comment) {
    this.id = id;
    this.name = name.getL();
    if (lessThan == null || lessThan.isEmpty()) {
      this.lessThan = new ArrayList<>();
    } else {
      this.lessThan = ImmutableList.copyOf(lessThan);
    }
    this.comment = comment;
  }

  public long getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public List<String> getLessThan() {
    return lessThan;
  }
}
