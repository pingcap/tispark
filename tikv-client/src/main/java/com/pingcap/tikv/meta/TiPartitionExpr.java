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

import com.pingcap.tikv.expression.Expression;
import java.io.Serializable;
import java.util.List;

public class TiPartitionExpr implements Serializable {
  private final List<Expression> ranges;
  private final List<Expression> upperBound;
  private final Expression column;

  public TiPartitionExpr(List<Expression> ranges, List<Expression> upperBound, Expression column) {
    this.ranges = ranges;
    this.upperBound = upperBound;
    this.column = column;
  }

  public Expression getColumn() {
    return column;
  }

  public List<Expression> getUpperBound() {
    return upperBound;
  }

  public List<Expression> getRanges() {
    return ranges;
  }

  public boolean canPruned() {
    return column == null;
  }
}
