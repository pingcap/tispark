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

package com.pingcap.tikv.expression;

import com.pingcap.tikv.types.DataType;
import java.io.Serializable;
import java.util.List;

public abstract class Expression implements Serializable {
  protected DataType dataType;
  protected boolean resolved;

  public Expression(DataType dataType) {
    this.dataType = dataType;
    this.resolved = true;
  }

  public Expression() {
    this.resolved = false;
  }

  public abstract List<Expression> getChildren();

  public abstract <R, C> R accept(Visitor<R, C> visitor, C context);

  public boolean isResolved() {
    return getChildren().stream().allMatch(Expression::isResolved);
  }

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }
}
