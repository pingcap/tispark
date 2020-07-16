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

public class TiSequenceInfo implements Serializable {
  private final long sequenceStart;
  private final boolean sequenceCache;
  private final boolean sequenceCycle;
  private final long sequenceMinValue;
  private final long sequenceMaxValue;
  private final long sequenceIncrement;
  private final long sequenceCacheValue;
  private final String sequenceComment;

  @JsonCreator
  public TiSequenceInfo(
      @JsonProperty("sequence_start") long sequenceStart,
      @JsonProperty("sequence_cache") boolean sequenceCache,
      @JsonProperty("sequence_cycle") boolean sequenceCycle,
      @JsonProperty("sequence_min_value") long sequenceMinValue,
      @JsonProperty("sequence_max_value") long sequenceMaxValue,
      @JsonProperty("sequence_increment") long sequenceIncrement,
      @JsonProperty("sequence_cache_value") long sequenceCacheValue,
      @JsonProperty("sequence_comment") String sequenceComment) {
    this.sequenceStart = sequenceStart;
    this.sequenceCache = sequenceCache;
    this.sequenceCycle = sequenceCycle;
    this.sequenceMinValue = sequenceMinValue;
    this.sequenceMaxValue = sequenceMaxValue;
    this.sequenceIncrement = sequenceIncrement;
    this.sequenceCacheValue = sequenceCacheValue;
    this.sequenceComment = sequenceComment;
  }
}
