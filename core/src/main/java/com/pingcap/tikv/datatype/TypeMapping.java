/*
 * Copyright 2020 PingCAP, Inc.
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

package com.pingcap.tikv.datatype;

import static com.pingcap.tikv.types.MySQLType.TypeLonglong;

import com.pingcap.tikv.types.AbstractDateTimeType;
import com.pingcap.tikv.types.BytesType;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DateType;
import com.pingcap.tikv.types.DecimalType;
import com.pingcap.tikv.types.EnumType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.JsonType;
import com.pingcap.tikv.types.RealType;
import com.pingcap.tikv.types.SetType;
import com.pingcap.tikv.types.StringType;
import com.pingcap.tikv.types.TimeType;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TypeMapping {
  private static final Logger logger = LoggerFactory.getLogger(TypeMapping.class.getName());
  private static final int MAX_PRECISION = 38;

  private static boolean isStringType(DataType type) {
    return type instanceof EnumType
        || type instanceof JsonType
        || type instanceof SetType
        || type instanceof StringType;
  }

  public static org.apache.spark.sql.types.DataType toSparkType(DataType type) {
    if (type instanceof DateType) {
      return DataTypes.DateType;
    }

    if (type instanceof AbstractDateTimeType) {
      return DataTypes.TimestampType;
    }

    if (type instanceof DecimalType) {
      int len = (int) type.getLength();
      if (len > MAX_PRECISION) {
        logger.warn(
            "Decimal precision exceeding MAX_PRECISION="
                + MAX_PRECISION
                + ", "
                + "value will be truncated");
        len = MAX_PRECISION;
      }

      return DataTypes.createDecimalType(len, type.getDecimal());
    }

    if (isStringType(type)) {
      return DataTypes.StringType;
    }

    if (type instanceof RealType) {
      switch (type.getType()) {
        case TypeFloat:
          return DataTypes.FloatType;
        case TypeDouble:
          return DataTypes.DoubleType;
      }
    }

    if (type instanceof BytesType) {
      return DataTypes.BinaryType;
    }

    if (type instanceof IntegerType) {
      // cast unsigned long to decimal to avoid potential overflow.
      if (type.isUnsigned() && type.getType() == TypeLonglong) {
        return DataTypes.createDecimalType(20, 0);
      }
      return DataTypes.LongType;
    }

    if (type instanceof TimeType) {
      return DataTypes.LongType;
    }

    throw new UnsupportedOperationException(
        String.format("found unsupported type %s", type.getClass().getCanonicalName()));
  }
}
