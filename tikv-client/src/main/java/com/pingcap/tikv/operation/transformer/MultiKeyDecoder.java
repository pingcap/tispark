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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType;
import java.util.List;

public class MultiKeyDecoder implements Projection {
  public MultiKeyDecoder(List<DataType> dataTypes) {
    this.resultTypes = requireNonNull(dataTypes).toArray(new DataType[0]);
  }

  private DataType[] resultTypes;

  @Override
  public void set(Object value, Row row, int pos) {
    byte[] rowData = (byte[]) value;
    CodecDataInput cdi = new CodecDataInput(rowData);

    for (int i = 0; i < resultTypes.length; i++) {
      resultTypes[i].decodeValueToRow(cdi, row, i + pos);
    }
  }

  @Override
  public int size() {
    return resultTypes.length;
  }

  @Override
  public List<DataType> getTypes() {
    return ImmutableList.copyOf(resultTypes);
  }
}
