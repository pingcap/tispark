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

import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;

public class TiColumnRef implements TiExpr {
  private long offset = -1;

  public static TiColumnInfo getColumnWithName(String name, TiTableInfo table) {
    TiColumnInfo columnInfo = null;
    for (TiColumnInfo col : table.getColumns()) {
      if (col.matchName(name)) {
        columnInfo = col;
        break;
      }
    }
    return columnInfo;
  }

  public static TiColumnRef create(String name, TiTableInfo table) {
    TiColumnRef ref = new TiColumnRef(name);
    ref.resolve(table);
    return ref;
  }

  public static TiColumnRef create(TiColumnInfo columnInfo, TiTableInfo table) {
    return new TiColumnRef(columnInfo.getName(), columnInfo, table);
  }

  public static TiColumnRef create(String name) {
    return new TiColumnRef(name);
  }

  private final String name;

  private TiColumnInfo columnInfo;
  private TiTableInfo tableInfo;

  private TiColumnRef(String name) {
    this.name = name;
  }

  private TiColumnRef(String name, TiColumnInfo columnInfo, TiTableInfo tableInfo) {
    this.name = name;
    this.columnInfo = columnInfo;
    this.tableInfo = tableInfo;
  }

  @Override
  public Expr toProto() {
    Expr.Builder builder = Expr.newBuilder();
    builder.setTp(ExprType.ColumnRef);
    CodecDataOutput cdo = new CodecDataOutput();
    // After switching to DAG request mode, expression value
    // should be the index of table columns we provided in
    // the first executor of a DAG request.
    //
    // If offset < 0, it's not a valid offset specified by
    // user, use columnInfo instead
    IntegerType.writeLong(cdo, offset < 0 ? columnInfo.getOffset() : offset);
    builder.setVal(cdo.toByteString());
    return builder.build();
  }

  @Override
  public DataType getType() {
    return columnInfo.getType();
  }

  @Override
  public TiColumnRef resolve(TiTableInfo table) {
    TiColumnInfo columnInfo = getColumnWithName(name, table);
    if (columnInfo == null) {
      throw new TiExpressionException("No Matching columns from " + table.getName());
    }

    // TODO: After type system finished, do a type check
    //switch column.GetType().Tp {
    //    case mysql.TypeBit, mysql.TypeSet, mysql.TypeEnum, mysql.TypeGeometry, mysql.TypeUnspecified:
    //        return nil
    //}

    if (columnInfo.getId() == 0) {
      throw new TiExpressionException("Zero Id is not a referable column id");
    }
    this.tableInfo = table;
    this.columnInfo = columnInfo;
    return this;
  }

  public String getName() {
    return name;
  }

  public TiColumnInfo getColumnInfo() {
    if (columnInfo == null) {
      throw new TiClientInternalException("ColumnRef is unbound");
    }
    return columnInfo;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public long getOffset() {
    return offset;
  }

  @Override
  public boolean equals(Object another) {
    if (this == another) {
      return true;
    }

    if (another instanceof TiColumnRef) {
      TiColumnRef tiColumnRef = (TiColumnRef) another;
      return tiColumnRef.columnInfo.getId() == this.columnInfo.getId()
          && tiColumnRef.getName().equalsIgnoreCase(this.columnInfo.getName())
          && tiColumnRef.tableInfo.getId() == this.tableInfo.getId();
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = result * prime + Long.hashCode(columnInfo.getId());
    result = result * prime + tableInfo.getName().hashCode();
    result = result * prime + columnInfo.getName().hashCode();
    result = result * prime + Long.hashCode(tableInfo.getId());
    return result;
  }

  @Override
  public String toString() {
    return String.format("[%s]", getName());
  }
}
