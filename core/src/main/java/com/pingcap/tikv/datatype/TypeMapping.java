package com.pingcap.tikv.datatype;

import static com.pingcap.tikv.types.MySQLType.TypeLonglong;

import com.pingcap.tikv.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class TypeMapping {

  public static org.apache.spark.sql.types.DataType toSparkType(DataType type) {
    switch (type.getType()) {
      case TypeDecimal:
        throw new UnsupportedOperationException("TypeDecimal cannot be used in calculation");
      case TypeBlob:
      case TypeLongBlob:
      case TypeTinyBlob:
      case TypeMediumBlob:
        return DataTypes.BinaryType;
      case TypeNewDate:
      case TypeDate:
        return DataTypes.DateType;
      case TypeDatetime:
      case TypeTimestamp:
        return DataTypes.TimestampType;
      case TypeFloat:
      case TypeDouble:
        return DataTypes.DoubleType;
      case TypeVarchar:
      case TypeString:
      case TypeSet:
      case TypeEnum:
      case TypeJSON:
      case TypeVarString:
        return DataTypes.StringType;
      case TypeNull:
        throw new UnsupportedOperationException("TypeNull is not supported");
      case TypeBit:
      case TypeTiny:
      case TypeYear:
      case TypeShort:
      case TypeInt24:
      case TypeDuration:
      case TypeLong:
      case TypeLonglong:
        if (type.isUnsigned() && type.getType() == TypeLonglong) {
          return DataTypes.createDecimalType(20, 0);
        }
        return DataTypes.LongType;
      case TypeGeometry:
        throw new UnsupportedOperationException("TypeGeometry is not supported");

      case TypeNewDecimal:
        return DataTypes.createDecimalType((int) type.getLength(), type.getDecimal());
    }
    throw new UnsupportedOperationException(
        String.format("Unsupported data type: %s", type.getType()));
  }
}