package org.apache.spark.sql.execution

import java.util.logging.Logger

import com.google.common.primitives.UnsignedLong
import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.operation.transformer.RowTransformer
import com.pingcap.tikv.types._
import com.pingcap.tispark.TiBatchWrite.TiRow
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Decimal}

object TiConverter {
  type TiDataType = com.pingcap.tikv.types.DataType
  type SparkSQLDataType = org.apache.spark.sql.types.DataType

  private final val logger = Logger.getLogger(getClass.getName)
  private final val MAX_PRECISION = sql.types.DecimalType.MAX_PRECISION

  def toSparkRow(row: TiRow, rowTransformer: RowTransformer): Row = {
    import scala.collection.JavaConversions._

    val finalTypes = rowTransformer.getTypes.toList
    val transRow = rowTransformer.transform(row)
    val rowArray = new Array[Any](finalTypes.size)

    for (i <- 0 until transRow.fieldCount) {
      val colTp = finalTypes(i)
      val isBigInt = colTp.getType.equals(MySQLType.TypeLonglong)
      val isUnsigned = colTp.isUnsigned
      val tmp = transRow.get(i, finalTypes(i))
      rowArray(i) = if (isBigInt && isUnsigned) {
        tmp match {
          case l: java.lang.Long => Decimal.apply(UnsignedLong.fromLongBits(l).bigIntegerValue())
          case _                 => tmp
        }
      } else {
        tmp
      }
    }

    Row.fromSeq(rowArray)
  }

  def fromSparkType(tp: SparkSQLDataType): TiDataType =
    // TODO: review type system
    // pending: https://internal.pingcap.net/jira/browse/TISPARK-99
    tp match {
      case _: sql.types.BinaryType  => BytesType.BLOB
      case _: sql.types.StringType  => StringType.VARCHAR
      case _: sql.types.LongType    => IntegerType.BIGINT
      case _: sql.types.IntegerType => IntegerType.INT
      case _: sql.types.DoubleType  => RealType.DOUBLE
      case sql.types.DecimalType.Fixed(prec, scale) =>
        new DecimalType(MySQLType.TypeNewDecimal, prec, scale)
      case _: sql.types.TimestampType => TimestampType.TIMESTAMP
      case _: sql.types.DateType      => DateType.DATE
    }

  /**
   * Convert from Spark SQL Supported Java Type to TiDB Type
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
   * @param value
   * @return
   */
  def sparkSQLObjectToJavaObject(value: Any): java.lang.Object = {
    if (value == null) {
      return null
    }

    import scala.collection.JavaConversions._
    val result: java.lang.Object = value match {
      case v: java.lang.Boolean    => v
      case v: java.lang.Byte       => v
      case v: java.lang.Short      => v
      case v: java.lang.Integer    => v
      case v: java.lang.Long       => v
      case v: java.lang.Float      => v
      case v: java.lang.Double     => v
      case v: java.lang.String     => v
      case v: java.math.BigDecimal => v
      case v: java.sql.Date        => v
      case v: java.sql.Timestamp   => v
      case v: Array[Byte] =>
        val r: java.util.List[java.lang.Byte] = v.toList.map(b => java.lang.Byte.valueOf(b))
        r
      // TODO: to support following types
      //case v: scala.collection.Seq[_] =>
      //case v: scala.collection.Map[_, _] =>
      //case v: org.apache.spark.sql.Row   =>
      case _ =>
        throw new TiBatchWriteException(
          s"do not support converting SparkSQL Data Type ${value.getClass} to TiDB Data Type!"
        )
    }
    result
  }
}
