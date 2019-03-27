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

package com.pingcap.tikv.txn.type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BatchKeys {
  private List<byte[]> keys;

  private Long regioId;

  public BatchKeys() {}

  public BatchKeys(Long regioId, List<byte[]> keysInput) {
    Objects.nonNull(regioId);
    Objects.nonNull(keysInput);
    this.regioId = regioId;
    this.keys = new ArrayList<>();
    this.keys.addAll(keysInput);
  }

  public List<byte[]> getKeys() {
    return keys;
  }

  public void setKeys(List<byte[]> keys) {
    this.keys = keys;
  }

  public Long getRegioId() {
    return regioId;
  }

  public void setRegioId(Long regioId) {
    this.regioId = regioId;
  }

  public byte[][] getKeysArray() {
    byte[][] result = new byte[keys.size()][];
    keys.toArray(result);
    return result;
  }
}
