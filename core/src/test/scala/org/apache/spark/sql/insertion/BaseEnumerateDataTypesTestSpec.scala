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

  // this only generate schema with one unique index
  def genSchema(dataTypes: List[ReflectedDataType], tablePrefix: String): List[Schema] = {
    val indices = genIndex(dataTypes, r)

    val dataTypesWithDescription = dataTypes.map { dataType =>
      val len = getTypeLength(dataType)
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

  private def toString(dataTypes: Seq[String]): String = dataTypes.mkString("[", ",", "]")

  override val rowCount = 50

  // we are not using below function, we probably need decouple the logic.
  override def getTableName(dataTypes: String*): String = ???

  override def getTableNameWithDesc(desc: String, dataTypes: String*): String = ???

  override def getIndexName(dataTypes: String*): String = ???
}
