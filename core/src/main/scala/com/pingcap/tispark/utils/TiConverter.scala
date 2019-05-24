package com.pingcap.tispark.utils

import java.util.logging.Logger

import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.meta.TiColumnInfo
import com.pingcap.tikv.operation.transformer.RowTransformer
import com.pingcap.tikv.types._
import com.pingcap.tispark.TiBatchWrite.TiRow
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataTypes

object TiConverter {
  type TiDataType = com.pingcap.tikv.types.DataType
  type SparkSQLDataType = org.apache.spark.sql.types.DataType

  private final val logger = Logger.getLogger(getClass.getName)
  private final val MAX_PRECISION = sql.types.DecimalType.MAX_PRECISION

  private final val MaxInt8: Long = (1L << 7) - 1
  private final val MinInt8: Long = -1L << 7
  private final val MaxInt16: Long = (1L << 15) - 1
  private final val MinInt16: Long = -1L << 15
  private final val MaxInt24: Long = (1L << 23) - 1
  private final val MinInt24: Long = -1L << 23
  private final val MaxInt32: Long = (1L << 31) - 1
  private final val MinInt32: Long = -1L << 31
  private final val MaxInt64: Long = (1L << 63) - 1
  private final val MinInt64: Long = -1L << 63
  private final val MaxUint8: Long = (1L << 8) - 1
  private final val MaxUint16: Long = (1L << 16) - 1
  private final val MaxUint24: Long = (1L << 24) - 1
  private final val MaxUint32: Long = (1L << 32) - 1
  private final val MaxUint64: Long = -1L

  def toSparkRow(row: TiRow, rowTransformer: RowTransformer): Row = {
    import scala.collection.JavaConversions._

    val finalTypes = rowTransformer.getTypes.toList
    val transRow = rowTransformer.transform(row)
    val rowArray = new Array[Any](finalTypes.size)

    for (i <- 0 until transRow.fieldCount) {
      rowArray(i) = transRow.get(i, finalTypes(i))
    }

    Row.fromSeq(rowArray)
  }

  // convert tikv-java client FieldType to Spark DataType
  def toSparkDataType(tp: TiDataType): SparkSQLDataType =
    tp match {
      case _: StringType => sql.types.StringType
      case _: BytesType  => sql.types.BinaryType
      case _: IntegerType =>
        if (tp.asInstanceOf[IntegerType].isUnsignedLong) {
          DataTypes.createDecimalType(20, 0)
        } else {
          sql.types.LongType
        }
      case _: RealType => sql.types.DoubleType
      // we need to make sure that tp.getLength does not result in negative number when casting.
      // Decimal precision cannot exceed MAX_PRECISION.
      case _: DecimalType =>
        var len = tp.getLength
        if (len > MAX_PRECISION) {
          logger.warning(
            "Decimal precision exceeding MAX_PRECISION=" + MAX_PRECISION + ", value will be truncated"
          )
          len = MAX_PRECISION
        }
        DataTypes.createDecimalType(
          len.asInstanceOf[Int],
          tp.getDecimal
        )
      case _: DateTimeType  => sql.types.TimestampType
      case _: TimestampType => sql.types.TimestampType
      case _: DateType      => sql.types.DateType
      case _: EnumType      => sql.types.StringType
      case _: SetType       => sql.types.StringType
      case _: JsonType      => sql.types.StringType
      case _: TimeType      => sql.types.LongType
    }

  def fromSparkType(tp: SparkSQLDataType): TiDataType =
    // TODO: review type system
    // pending: https://internal.pingcap.net/jira/browse/TISPARK-99
    tp match {
      case _: sql.types.BinaryType    => BytesType.BLOB
      case _: sql.types.StringType    => StringType.VARCHAR
      case _: sql.types.LongType      => IntegerType.BIGINT
      case _: sql.types.IntegerType   => IntegerType.INT
      case _: sql.types.DoubleType    => RealType.DOUBLE
      case _: sql.types.DecimalType   => DecimalType.DECIMAL
      case _: sql.types.TimestampType => TimestampType.TIMESTAMP
      case _: sql.types.DateType      => DateType.DATE
    }

  /**
   * Convert from Spark SQL Supported Java Type to TiDB Type
   *
   * 1. data convert, e.g. Integer -> SHORT
   * 2. check overflow, e.g. write 1000 to short
   *
   *
   * Spark SQL only support following types:
   *
   * 1. BooleanType -> java.lang.Boolean
   * 2. ByteType -> java.lang.Byte
   * 3. ShortType -> java.lang.Short
   * 4. IntegerType -> java.lang.Integer
   * 5. LongType -> java.lang.Long
   * 6. FloatType -> java.lang.Float
   * 7. DoubleType -> java.lang.Double
   * 8. StringType -> String
   * 9. DecimalType -> java.math.BigDecimal
   * 10. DateType -> java.sql.Date
   * 11. TimestampType -> java.sql.Timestamp
   * 12. BinaryType -> byte array
   * 13. ArrayType -> scala.collection.Seq (use getList for java.util.List)
   * 14. MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
   * 15. StructType -> org.apache.spark.sql.Row
   *
   * @param targetColumnInfo
   * @param value
   * @return
   */
  @throws[TiBatchWriteException]
  def convertToTiDBType(targetColumnInfo: TiColumnInfo, value: AnyRef): Object = {
    if (value == null) {
      return null
    }

    targetColumnInfo.getType match {
      //case _: BitType       =>
      //case _: BytesType     =>
      //case _: DateTimeType  =>
      // TODO: add test for DateType
      case _: DateType => value
      // TODO: add test for DecimalType
      case _: DecimalType => value
      //case _: EnumType      =>
      case _: IntegerType =>
        if (targetColumnInfo.getType.isUnsigned) {
          convertToUnsigned(targetColumnInfo, value)
        } else {
          convertToSigned(targetColumnInfo, value)
        }
      //case JsonType      =>
      case _: RealType => convertToReal(targetColumnInfo, value)
      //case SetType       =>
      // TODO: add test for StringType
      case _: StringType => value
      //case _: TimeType      =>
      //case _: TimestampType =>
      case _ =>
        throw new TiBatchWriteException(
          s"do not support writing to column type: ${targetColumnInfo.getType}"
        )
    }
  }

  private def convertToSigned(targetColumnInfo: TiColumnInfo, value: AnyRef): Object = {
    val lowerBound = integerSignedLowerBound(targetColumnInfo.getType)
    val upperBound = integerSignedUpperBound(targetColumnInfo.getType)

    val result: java.lang.Long = value match {
      case v: java.lang.Boolean => if (v) 1L else 0L
      case v: java.lang.Byte    => v.longValue()
      case v: java.lang.Short   => v.longValue()
      case v: java.lang.Integer => v.longValue()
      case v: java.lang.Long    => v.longValue()
      case v: java.lang.Float   => floatToLong(v)
      case v: java.lang.Double  => doubleToLong(v)
      case v: String            => stringToLong(v)
      // TODO: case v: java.math.BigDecimal => v.longValue()
      // TODO: support following types
      //case v: java.sql.Date              =>
      //case v: java.sql.Timestamp         =>
      //case v: Array[String]              =>
      //case v: scala.collection.Seq[_]    =>
      //case v: scala.collection.Map[_, _] =>
      //case v: org.apache.spark.sql.Row   =>
      case _ =>
        throw new TiBatchWriteException(
          s"do not support converting from ${value.getClass} to column type: ${targetColumnInfo.getType}"
        )
    }

    if (result < lowerBound) {
      throw new TiBatchWriteException(
        s"data $value < lowerBound $lowerBound"
      )
    }

    if (result > upperBound) {
      throw new TiBatchWriteException(
        s"data $value > upperBound $upperBound"
      )
    }

    result
  }

  private def convertToUnsigned(targetColumnInfo: TiColumnInfo, value: AnyRef): Object = {
    val lowerBound = 0L
    val upperBound = integerUnsignedUpperBound(targetColumnInfo.getType)

    val result: java.lang.Long = value match {
      case v: java.lang.Boolean => if (v) 1L else 0L
      case v: java.lang.Byte    => v.longValue()
      case v: java.lang.Short   => v.longValue()
      case v: java.lang.Integer => v.longValue()
      case v: java.lang.Long    => v.longValue()
      case v: java.lang.Float   => floatToLong(v)
      case v: java.lang.Double  => doubleToLong(v)
      case v: String            => stringToLong(v)
      // TODO: case v: java.math.BigDecimal => v.longValue()
      // TODO: support following types
      //case v: java.sql.Date              =>
      //case v: java.sql.Timestamp         =>
      //case v: Array[String]              =>
      //case v: scala.collection.Seq[_]    =>
      //case v: scala.collection.Map[_, _] =>
      //case v: org.apache.spark.sql.Row   =>
      case _ =>
        throw new TiBatchWriteException(
          s"do not support converting from ${value.getClass} to column type: ${targetColumnInfo.getType}"
        )
    }

    if (result < lowerBound) {
      throw new TiBatchWriteException(
        s"data $value < lowerBound $lowerBound"
      )
    }

    if (java.lang.Long.compareUnsigned(result, upperBound) > 0) {
      throw new TiBatchWriteException(
        s"data $value > upperBound $upperBound"
      )
    }

    result
  }

  private def convertToReal(targetColumnInfo: TiColumnInfo, value: AnyRef): Object = {
    val result: java.lang.Double = value match {
      case v: java.lang.Boolean => if (v) 1d else 0d
      case v: java.lang.Byte    => v.doubleValue()
      case v: java.lang.Short   => v.doubleValue()
      case v: java.lang.Integer => v.doubleValue()
      case v: java.lang.Long    => v.doubleValue()
      case v: java.lang.Float   => v.doubleValue()
      case v: java.lang.Double  => v
      case v: String            => stringToDouble(v)
      // TODO: case v: java.math.BigDecimal => v.doubleValue()
      // TODO: support following types
      //case v: java.sql.Date              =>
      //case v: java.sql.Timestamp         =>
      //case v: Array[String]              =>
      //case v: scala.collection.Seq[_]    =>
      //case v: scala.collection.Map[_, _] =>
      //case v: org.apache.spark.sql.Row   =>
      case _ =>
        throw new TiBatchWriteException(
          s"do not support converting from ${value.getClass} to column type: ${targetColumnInfo.getType}"
        )
    }

    targetColumnInfo.getType.getType match {
      case MySQLType.TypeFloat =>
        if (java.lang.Double.compare(result, java.lang.Float.MAX_VALUE) > 0) {
          throw new TiBatchWriteException(
            s"data $value > upperBound ${java.lang.Float.MAX_VALUE}"
          )
        }

        if (java.lang.Double.compare(result, java.lang.Float.MIN_VALUE) < 0) {
          throw new TiBatchWriteException(
            s"data $value < lowerBound ${java.lang.Float.MIN_VALUE}"
          )
        }
        val r: java.lang.Float = result.toFloat
        r
      case _ => result
    }
  }

  private def floatToLong(v: java.lang.Float): java.lang.Long =
    Math.round(v).longValue()

  private def doubleToLong(v: java.lang.Double): java.lang.Long =
    Math.round(v)

  private def stringToDouble(v: String): java.lang.Double =
    java.lang.Double.parseDouble(v)

  private def stringToLong(v: String): java.lang.Long =
    doubleToLong(stringToDouble(v))

  private def integerSignedLowerBound(dataType: TiDataType): Long =
    dataType.getType match {
      case MySQLType.TypeTiny     => MinInt8
      case MySQLType.TypeShort    => MinInt16
      case MySQLType.TypeInt24    => MinInt24
      case MySQLType.TypeLong     => MinInt32
      case MySQLType.TypeLonglong => MinInt64
      case _ =>
        throw new TiBatchWriteException(
          s"Input Type is not a mysql type"
        )
    }

  private def integerSignedUpperBound(dataType: TiDataType): Long =
    dataType.getType match {
      case MySQLType.TypeTiny     => MaxInt8
      case MySQLType.TypeShort    => MaxInt16
      case MySQLType.TypeInt24    => MaxInt24
      case MySQLType.TypeLong     => MaxInt32
      case MySQLType.TypeLonglong => MaxInt64
      case _ =>
        throw new TiBatchWriteException(
          s"Input Type is not a mysql type"
        )
    }

  private def integerUnsignedUpperBound(dataType: TiDataType): Long =
    dataType.getType match {
      case MySQLType.TypeTiny     => MaxUint8
      case MySQLType.TypeShort    => MaxUint16
      case MySQLType.TypeInt24    => MaxUint24
      case MySQLType.TypeLong     => MaxUint32
      case MySQLType.TypeLonglong => MaxUint64
      case MySQLType.TypeBit      => MaxUint64
      case MySQLType.TypeEnum     => MaxUint64
      case MySQLType.TypeSet      => MaxUint64
      case _ =>
        throw new TiBatchWriteException(
          s"Input Type is not a mysql type"
        )
    }
}
