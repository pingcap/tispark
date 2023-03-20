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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.tikv.shade.com.google.common.annotations.VisibleForTesting;
import org.tikv.shade.com.google.common.collect.ImmutableList;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TiPartitionDef implements Serializable {
  private final long id;
  private final String name;
  private final List<String> lessThan;
  private final List<List<String>> inValues;
  private final String comment;

  @VisibleForTesting
  @JsonCreator
  public TiPartitionDef(
      @JsonProperty("id") long id,
      @JsonProperty("name") CIStr name,
      @JsonProperty("less_than") List<String> lessThan,
      @JsonProperty("in_values") List<List<String>> inValues,
      @JsonProperty("comment") String comment) {
    this.id = id;
    this.name = name.getL();
    if (lessThan == null || lessThan.isEmpty()) {
      this.lessThan = new ArrayList<>();
    } else {
      this.lessThan = ImmutableList.copyOf(lessThan);
    }
    if (inValues == null || inValues.isEmpty()) {
      this.inValues = new ArrayList<>();
    } else {
      this.inValues = ImmutableList.copyOf(inValues);
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

  public List<List<String>> getInValues() {
    return inValues;
  }
}
