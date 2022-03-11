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
import com.google.common.primitives.UnsignedLong;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.types.BytesType;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DateTimeType;
import com.pingcap.tikv.types.DateType;
import com.pingcap.tikv.types.DecimalType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.RealType;
import com.pingcap.tikv.types.StringType;
import com.pingcap.tikv.types.TimestampType;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;
import org.joda.time.DateTime;

// Refactor needed.
// Refer to https://github.com/pingcap/tipb/blob/master/go-tipb/expression.pb.go
// TODO: This might need a refactor to accept an DataType?
public class Constant extends Expression {
  private final Object value;
  private final BigDecimal UNSIGNED_LONG_MAX =
      new BigDecimal(UnsignedLong.fromLongBits(-1).toString());

  public Constant(Object value, DataType type) {
    this.value = value;
    this.dataType = (type == null && value != null) ? getDefaultType(value) : type;
    this.resolved = true;
  }

  public static Constant create(Object value, DataType type) {
    return new Constant(value, type);
  }

  @Deprecated
  public static Constant create(Object value) {
    return new Constant(value, null);
  }

  protected static boolean isIntegerType(Object value) {
    return value instanceof Long
        || value instanceof Integer
        || value instanceof Short
        || value instanceof Byte;
  }

  private static DataType getDefaultType(Object value) {
    if (value == null) {
      throw new TiExpressionException("NULL constant has no type");
    } else if (isIntegerType(value)) {
      return IntegerType.BIGINT;
    } else if (value instanceof String) {
      return StringType.VARCHAR;
    } else if (value instanceof Float) {
      return RealType.FLOAT;
    } else if (value instanceof Double) {
      return RealType.DOUBLE;
    } else if (value instanceof BigDecimal) {
      int prec = ((BigDecimal) value).precision();
      int frac = ((BigDecimal) value).scale();
      return new DecimalType(prec, frac);
    } else if (value instanceof DateTime) {
      return DateTimeType.DATETIME;
    } else if (value instanceof Date) {
      return DateType.DATE;
    } else if (value instanceof Timestamp) {
      return TimestampType.TIMESTAMP;
    } else if (value instanceof byte[]) {
      return BytesType.TEXT;
    } else {
      throw new TiExpressionException(
          "Constant type not supported:" + value.getClass().getSimpleName());
    }
  }

  public void setType(DataType type) {
    this.dataType = type;
  }

  public Object getValue() {
    return value;
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
    if (other instanceof Constant) {
      return Objects.equals(value, ((Constant) other).value);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

  @Override
  public List<Expression> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public <R, C> R accept(Visitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  public boolean isOverflowed() {
    if (this.dataType instanceof IntegerType) {
      if (((IntegerType) this.dataType).isUnsignedLong()) {
        return ((BigDecimal) value).min(UNSIGNED_LONG_MAX).signum() > 0
            || ((BigDecimal) value).signum() < 0;
      }
    }
    return false;
  }
}
