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
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import java.util.List;
import java.util.Objects;

public class ColumnRef extends Expression {
  public static ColumnRef create(String name, TiTableInfo table) {
    for (TiColumnInfo columnInfo : table.getColumns()) {
      if (columnInfo.matchName(name)) {
        return new ColumnRef(columnInfo.getName(), columnInfo, table);
      }
    }
    throw new TiExpressionException(
        String.format("Column name %s not found in table %s", name, table));
  }

  public static ColumnRef create(String name) {
    return new ColumnRef(name);
  }

  private final String name;

  private TiColumnInfo columnInfo;
  private TiTableInfo tableInfo;

  public ColumnRef(String name) {
    this.name = name;
  }

  public ColumnRef(String name, TiColumnInfo columnInfo, TiTableInfo tableInfo) {
    this.name = name;
    this.columnInfo = columnInfo;
    this.tableInfo = tableInfo;
  }

  public String getName() {
    return name;
  }

  public void resolve(TiTableInfo table) {
    TiColumnInfo columnInfo = null;
    for (TiColumnInfo col : table.getColumns()) {
      if (col.matchName(name)) {
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

    this.tableInfo = table;
    this.columnInfo = columnInfo;
  }

  public TiColumnInfo getColumnInfo() {
    if (columnInfo == null) {
      throw new TiClientInternalException(String.format("ColumnRef [%s] is unbound", name));
    }
    return columnInfo;
  }

  public DataType getType() {
    return getColumnInfo().getType();
  }

  public TiTableInfo getTableInfo() {
    return tableInfo;
  }

  public boolean isResolved() {
    return tableInfo != null && columnInfo != null;
  }

  @Override
  public boolean equals(Object another) {
    if (this == another) {
      return true;
    }

    if (another instanceof ColumnRef) {
      ColumnRef that = (ColumnRef) another;
      if (isResolved() && that.isResolved()) {
        return Objects.equals(columnInfo, that.columnInfo)
            && Objects.equals(tableInfo, that.tableInfo);
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
      return Objects.hash(tableInfo, columnInfo);
    } else {
      return Objects.hashCode(name);
    }
  }

  @Override
  public String toString() {
    return String.format("[%s]", getName());
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
