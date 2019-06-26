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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.pingcap.tikv.types.DataType;
import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TiIndexColumn implements Serializable {
  private final String name;
  private final int offset;
  private final long length;

  @JsonCreator
  public TiIndexColumn(
      @JsonProperty("name") CIStr name,
      @JsonProperty("offset") int offset,
      @JsonProperty("length") long length) {
    this.name = name.getL();
    this.offset = offset;
    this.length = length;
  }

  public String getName() {
    return name;
  }

  public int getOffset() {
    return offset;
  }

  public long getLength() {
    return length;
  }

  public boolean isLengthUnspecified() {
    return DataType.isLengthUnSpecified(length);
  }

  public boolean isPrefixIndex() {
    return !isLengthUnspecified();
  }

  public boolean matchName(String otherName) {
    return name.equalsIgnoreCase(otherName);
  }

  @Override
  public String toString() {
    return String.format(
        "%s {name: %s, offset: %d, length: %d}", getClass().getSimpleName(), name, offset, length);
  }
}
