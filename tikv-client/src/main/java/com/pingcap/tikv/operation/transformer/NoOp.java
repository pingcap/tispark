/*
 *
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
 *
 */

package com.pingcap.tikv.operation.transformer;

import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType;

/** Noop is a base type projection, it basically do nothing but copy. */
public class NoOp implements Projection {
  protected DataType targetDataType;

  public NoOp(DataType dataType) {
    this.targetDataType = dataType;
  }

  @Override
  public void set(Object value, Row row, int pos) {
    row.set(pos, targetDataType, value);
  }

  @Override
  public int size() {
    return 1;
  }

  @Override
  public DataType getType() {
    return targetDataType;
  }
}
