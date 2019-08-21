package org.apache.spark.sql.types

import org.apache.spark.sql.TiSparkTestSpec
import org.apache.spark.sql.test.generator.DataType.ReflectedDataType

trait MultiColumnDataTypeTestSpec extends TiSparkTestSpec {
  val dataTypes: List[ReflectedDataType]
  val unsignedDataTypes: List[ReflectedDataType]

  val extraDesc = "unsigned"
}
