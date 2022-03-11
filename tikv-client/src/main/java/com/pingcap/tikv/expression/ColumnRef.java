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

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import java.util.List;
import java.util.Objects;

public class ColumnRef extends Expression {
  private final String name;

  @Deprecated
  public ColumnRef(String name) {
    this.name = name;
  }

  public ColumnRef(String name, DataType dataType) {
    super(dataType);
    resolved = true;
    this.name = name;
  }

  public static ColumnRef create(String name, TiTableInfo table) {
    name = name.replaceAll("`", "");
    TiColumnInfo col = table.getColumn(name);
    if (col != null) {
      return new ColumnRef(name, col.getType());
    }

    throw new TiExpressionException(
        String.format("Column name %s not found in table %s", name, table));
  }

  @Deprecated
  public static ColumnRef create(String name) {
    return new ColumnRef(name);
  }

  public static ColumnRef create(String name, DataType dataType) {
    return new ColumnRef(name, dataType);
  }

  public static ColumnRef create(String name, TiColumnInfo columnInfo) {
    return new ColumnRef(name, columnInfo.getType());
  }

  public String getName() {
    return name.toLowerCase();
  }

  public void resolve(TiTableInfo table) {
    TiColumnInfo columnInfo = null;
    for (TiColumnInfo col : table.getColumns()) {
      if (col.matchName(name)) {
        this.dataType = col.getType();
        columnInfo = col;
        break;
      }
    }
    if (columnInfo == null) {
      throw new TiExpressionException(
          String.format("No Matching column %s from table %s", name, table.getName()));
    }

    if (columnInfo.getId() == 0) {
      throw new TiExpressionException("Zero Id is not a referable column id");
    }
  }

  public boolean matchName(String name) {
    return this.name.equalsIgnoreCase(name);
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  @Override
  public boolean isResolved() {
    return resolved;
  }

  @Override
  public boolean equals(Object another) {
    if (this == another) {
      return true;
    }

    if (another instanceof ColumnRef) {
      ColumnRef that = (ColumnRef) another;
      if (isResolved() && that.isResolved()) {
        return name.equalsIgnoreCase(that.name)
            && this.dataType.equals(((ColumnRef) another).dataType);
      } else {
        return name.equalsIgnoreCase(that.name);
      }

    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    if (isResolved()) {
      return Objects.hash(this.name, this.dataType);
    } else {
      return Objects.hashCode(name);
    }
  }

  @Override
  public String toString() {
    if (dataType != null) {
      return String.format("%s@%s", getName(), dataType.getName());
    } else {
      return String.format("[%s]", getName());
    }
  }

  @Override
  public List<Expression> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public <R, C> R accept(Visitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
