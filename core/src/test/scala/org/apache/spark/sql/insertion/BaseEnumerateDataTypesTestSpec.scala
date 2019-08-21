package org.apache.spark.sql.insertion

import org.apache.spark.sql.BaseTestGenerationSpec
import org.apache.spark.sql.test.generator.DataType.{getBaseType, DECIMAL, ReflectedDataType}
import org.apache.spark.sql.test.generator.TestDataGenerator._
import org.apache.spark.sql.test.generator.{Index, Schema}
import org.apache.spark.sql.types.MultiColumnDataTypeTestSpec

import scala.util.Random

trait BaseEnumerateDataTypesTestSpec
    extends MultiColumnDataTypeTestSpec
    with BaseTestGenerationSpec {
  def genIndex(dataTypes: List[ReflectedDataType], r: Random): List[List[Index]]

  def genLen(dataType: ReflectedDataType): String = {
    val baseType = getBaseType(dataType)
    val length = getLength(baseType)
    dataType match {
      case DECIMAL                       => s"$length,${getDecimal(baseType)}"
      case _ if isVarString(dataType)    => s"$length"
      case _ if isCharOrBinary(dataType) => "10"
      case _                             => ""
    }
  }

  // this only generate schema with one unique index
  def genSchema(dataTypes: List[ReflectedDataType], tablePrefix: String): List[Schema] = {
    val indices = genIndex(dataTypes, r)

    val dataTypesWithDescription = dataTypes.map { dataType =>
      val len = genLen(dataType)
      if (isNumeric(dataType)) {
        (dataType, len, "not null")
      } else {
        (dataType, len, "")
      }
    }

    indices.zipWithIndex.map { index =>
      schemaGenerator(
        database,
        // table name
        tablePrefix + index._2,
        r,
        dataTypesWithDescription,
        // constraint
        index._1
      )
    }
  }

  private def toString(dataTypes: Seq[String]): String = dataTypes.hashCode().toString

  override val rowCount = 50

  // we are not using below function, we probably need decouple the logic.
  override def getTableName(dataTypes: String*): String = ???

  override def getTableNameWithDesc(desc: String, dataTypes: String*): String = ???

  override def getIndexName(dataTypes: String*): String = ???
}
