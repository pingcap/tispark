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

package com.pingcap.tikv.types;

public enum RequestTypes {
  REQ_TYPE_SELECT(101),
  REQ_TYPE_INDEX(102),
  REQ_TYPE_DAG(103),
  REQ_TYPE_ANALYZE(104),
  BATCH_ROW_COUNT(64);

  private final int value;

  RequestTypes(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
