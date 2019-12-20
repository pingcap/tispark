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
import java.util.logging.Logger;
import org.apache.spark.sql.types.DataTypes;

public class TypeMapping {
  private final static Logger logger = Logger.getLogger(TypeMapping.class.getName());
  private final static int MAX_PRECISION = 38;

  private static boolean isStringType(DataType type) {
    return type instanceof EnumType || type instanceof JsonType || type instanceof SetType
        || type instanceof StringType;
  }

  public static org.apache.spark.sql.types.DataType toSparkType(DataType type) {
    if (type instanceof DateType) {
      return DataTypes.DateType;
    }

    if(type instanceof AbstractDateTimeType) {
      return DataTypes.TimestampType;
    }

    if (type instanceof DecimalType) {
      int len = (int) type.getLength();
      if (len > MAX_PRECISION) {
        logger.warning(
            "Decimal precision exceeding MAX_PRECISION=" + MAX_PRECISION + ", "
                + "value will be truncated");
        len = MAX_PRECISION;
      }

      return DataTypes.createDecimalType(len, type.getDecimal());
    }

    if (isStringType(type)) {
      return DataTypes.StringType;
    }

    if (type instanceof RealType) {
      switch (type.getType()){
        case TypeFloat:
          return DataTypes.FloatType;
        case TypeDouble:
          return DataTypes.DoubleType;
      }
    }

    if(type instanceof BytesType) {
      return DataTypes.BinaryType;
    }

    if (type instanceof IntegerType) {
      // cast unsigned long to decimal to avoid potential overflow.
      if (type.isUnsigned() && type.getType() == TypeLonglong) {
        return DataTypes.createDecimalType(20, 0);
      }
      return DataTypes.LongType;
    }

    if(type instanceof TimeType) {
      return DataTypes.LongType;
    }

    throw new UnsupportedOperationException(String.format("found unsupported type %s",
        type.getClass().getCanonicalName()));
  }


}