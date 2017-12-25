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
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.*;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Objects;

import static com.pingcap.tikv.types.Types.*;

// Refactor needed.
// Refer to https://github.com/pingcap/tipb/blob/master/go-tipb/expression.pb.go
// TODO: This might need a refactor to accept an DataType?
public class TiConstant implements TiExpr {
  public static class DateWrapper implements Serializable {
    private Long value;

    public DateWrapper(Long value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value == null ? "" : value.toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (obj instanceof DateWrapper) {
        return ((DateWrapper) obj).value.equals(value);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (int) (7 * (31 * value));
    }
  }

  private Object value;

  public static TiConstant create(Object value) {
    return new TiConstant(value);
  }

  private TiConstant(Object value) {
    this.value = value;
  }

  protected boolean isIntegerType() {
    return value instanceof Long
        || value instanceof Integer
        || value instanceof Short
        || value instanceof Byte;
  }

  public Object getValue() {
    return value;
  }

  // refer to expr_to_pb.go:datumToPBExpr
  // But since it's a java client, we ignored
  // unsigned types for now
  // TODO: Add unsigned constant types support
  @Override
  public Expr toProto() {
    Expr.Builder builder = Expr.newBuilder();
    CodecDataOutput cdo = new CodecDataOutput();
    // We don't allow build a unsigned long constant for now
    if (value == null) {
      builder.setTp(ExprType.Null);
    } else if (isIntegerType()) {
      builder.setTp(ExprType.Int64);
      IntegerType.writeLong(cdo, ((Number) value).longValue());
    } else if (value instanceof String) {
      builder.setTp(ExprType.String);
      // Instead of using BytesType codec, coprocessor reads
      // raw string as bytes
      cdo.write(((String) value).getBytes());
    } else if (value instanceof Float) {
      builder.setTp(ExprType.Float32);
      RealType.writeFloat(cdo, (Float) value);
    } else if (value instanceof Double) {
      builder.setTp(ExprType.Float64);
      RealType.writeDouble(cdo, (Double) value);
    } else if (value instanceof BigDecimal) {
      builder.setTp(ExprType.MysqlDecimal);
      DecimalType.writeDecimal(cdo, (BigDecimal) value);
    } else if (value instanceof DateWrapper) {
      builder.setTp(ExprType.MysqlTime);
      IntegerType.writeULong(cdo, calcTimestampFromTime(((DateWrapper) value).value));
    } else if (value instanceof Timestamp) {
      builder.setTp(ExprType.MysqlTime);
      IntegerType.writeULong(cdo, TimestampType.toPackedLong(((Timestamp) value).toLocalDateTime()));
    } else {
      throw new TiExpressionException("Constant type not supported.");
    }
    builder.setVal(cdo.toByteString());

    return builder.build();
  }

  @Override
  public DataType getType() {
    if (value == null) {
      throw new TiExpressionException("NULL constant has no type");
    } else if (isIntegerType()) {
      return DataTypeFactory.of(TYPE_LONG);
    } else if (value instanceof String) {
      return DataTypeFactory.of(TYPE_VARCHAR);
    } else if (value instanceof Float) {
      return DataTypeFactory.of(TYPE_FLOAT);
    } else if (value instanceof Double) {
      return DataTypeFactory.of(TYPE_DOUBLE);
    } else {
      throw new TiExpressionException("Constant type not supported.");
    }
  }

  private static long calcTimestampFromTime(Long time) {
    LocalDate jodaDate = new LocalDate(time, DateTimeZone.UTC);
    return TimestampType.toPackedLong(
        jodaDate.getYear(),
        jodaDate.getMonthOfYear(),
        jodaDate.getDayOfMonth(),
        0, 0, 0, 0
        // java.sql.Date does not provide these precision conceptually, need to set them 0
    );
  }

  @Override
  public TiConstant resolve(TiTableInfo table) {
    return this;
  }

  @Override
  public String toString() {
    if (value == null) {
      return "null";
    }
    if (value instanceof String) {
      return String.format("\"%s\"", value);
    }
    return value.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof TiConstant) {
      return Objects.equals(value, ((TiConstant) other).value);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return value == null ? 0 : value.hashCode();
  }
}
