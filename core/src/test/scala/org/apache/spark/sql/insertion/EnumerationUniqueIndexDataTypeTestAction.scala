package org.apache.spark.sql.insertion

import org.apache.commons.math3.util.Combinations
import org.apache.spark.sql.BaseTestGenerationSpec
import org.apache.spark.sql.test.generator.DataType.{getBaseType, DECIMAL, ReflectedDataType}
import org.apache.spark.sql.test.generator.TestDataGenerator.{getDecimal, getLength, isCharOrBinary, isNumeric, isStringType, isVarString, schemaGenerator}
import org.apache.spark.sql.test.generator._
import org.apache.spark.sql.types.MultiColumnDataTypeTestSpec

import scala.util.Random

trait EnumerationUniqueIndexDataTypeTestAction
    extends MultiColumnDataTypeTestSpec
    with BaseTestGenerationSpec {
  private def genIndex(dataTypes: List[ReflectedDataType], r: Random): List[Index] = {
    val size = dataTypes.length
    // the first step is generate all possible keys
    val keyList = scala.collection.mutable.ListBuffer.empty[Key]
    for (i <- 1 until 3) {
      val combination = new Combinations(size, i)
      //(i, size)
      val iterator = combination.iterator()
      while (iterator.hasNext) {
        val intArray = iterator.next()
        val indexColumnList = scala.collection.mutable.ListBuffer.empty[IndexColumn]
        // index may have multiple column
        for (j <- 0 until intArray.length) {
          // we add extra one to the column id since 1 is reserved to primary key
          if (isStringType(dataTypes(intArray(j)))) {
            indexColumnList += PrefixColumn(intArray(j) + 1, r.nextInt(4) + 2)
          } else {
            indexColumnList += DefaultColumn(intArray(j) + 1)
          }
        }

        keyList += Key(indexColumnList.toList)
      }
    }

    keyList.toList
  }

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
        tablePrefix + index._2,
        r,
        dataTypesWithDescription,
        List(index._1)
      )
    }
  }

  private def toString(dataTypes: Seq[String]): String = dataTypes.hashCode().toString

  override val rowCount = 10

  override def getTableName(dataTypes: String*): String = s"test_${toString(dataTypes)}"

  override def getTableNameWithDesc(desc: String, dataTypes: String*): String =
    s"test_${desc}_${toString(dataTypes)}"

  override def getIndexName(dataTypes: String*): String = s"idx_${toString(dataTypes)}"
}
