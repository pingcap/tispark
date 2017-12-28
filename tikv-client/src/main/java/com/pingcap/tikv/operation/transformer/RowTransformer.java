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
import com.pingcap.tikv.row.ObjectRowImpl;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * RowTransformer is used along with SchemaInfer and row and provide some operation. If you do not
 * know your target FieldType, then you do not need use this interface. The reason we provide this
 * interface is that sometime we need use it. Suppose we have a table t1 and have two column c1 and
 * s1 select sum(c1) from t1 will return SingleGroup literally and sum(c1). SingleGroup should be
 * skipped. Hence, skip operation is needed here. Another usage is that sum(c1)'s type is decimal no
 * matter what real column type is. We need cast it to target type which is column's type. Hence,
 * cast operation is needed. RowTransformer is executed after row is already read from
 * CodecDataInput.
 */
public class RowTransformer {
  public static Builder newBuilder() {
    return new Builder();
  }

  /** A Builder can build a RowTransformer. */
  public static class Builder {
    private final List<Projection> projections = new ArrayList<>();
    private final List<DataType> sourceTypes = new ArrayList<>();

    public RowTransformer build() {
      return new RowTransformer(sourceTypes, projections);
    }

    public Builder addProjection(Projection projection) {
      this.projections.add(projection);
      return this;
    }

    public Builder addProjections(Projection... projections) {
      this.projections.addAll(Arrays.asList(projections));
      return this;
    }

    public Builder addSourceFieldType(DataType fieldType) {
      this.sourceTypes.add(fieldType);
      return this;
    }

    public Builder addSourceFieldTypes(DataType... fieldTypes) {
      this.sourceTypes.addAll(Arrays.asList(fieldTypes));
      return this;
    }

    public Builder addSourceFieldTypes(List<DataType> fieldTypes) {
      this.sourceTypes.addAll(fieldTypes);
      return this;
    }
  }

  private final List<Projection> projections;

  private final List<DataType> sourceFieldTypes;

  private RowTransformer(List<DataType> sourceTypes, List<Projection> projections) {
    this.sourceFieldTypes = ImmutableList.copyOf(requireNonNull(sourceTypes));
    this.projections = ImmutableList.copyOf(requireNonNull(projections));
  }

  /**
   * Transforms input row to a output row according projections operator passed on creation of this
   * RowTransformer.
   *
   * @param inRow input row that need to be transformed.
   * @return a row that is already transformed.
   */
  public Row transform(Row inRow) {
    // After transform the length of row is probably not same as the input row.
    // we need calculate the new length.
    Row outRow = ObjectRowImpl.create(newRowLength());

    int offset = 0;
    for (int i = 0; i < inRow.fieldCount(); i++) {
      Object inVal = inRow.get(i, sourceFieldTypes.get(i));
      Projection p = getProjection(i);
      p.set(inVal, outRow, offset);
      offset += p.size();
    }
    return outRow;
  }

  private Projection getProjection(int index) {
    return projections.get(index);
  }

  /**
   * Collect output row's length.
   *
   * @return a int which is the new length of output row.
   */
  private int newRowLength() {
    return this.projections.stream().reduce(0, (sum, p) -> sum += p.size(), (s1, s2) -> s1 + s2);
  }

  public List<DataType> getTypes() {
    return projections
        .stream()
        .flatMap(proj -> proj.getTypes().stream())
        .collect(Collectors.toList());
  }
}
