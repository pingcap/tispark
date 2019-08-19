package org.apache.spark.sql.insertion

import org.apache.commons.math3.util.Combinations
import org.apache.spark.sql.BaseTestGenerationSpec
import org.apache.spark.sql.test.generator.DataType.{getBaseType, DECIMAL, ReflectedDataType}
import org.apache.spark.sql.test.generator.TestDataGenerator.{getDecimal, getLength, isCharOrBinary, isNumeric, isStringType, isVarString, schemaGenerator}
import org.apache.spark.sql.test.generator._
import org.apache.spark.sql.types.MultiColumnDataTypeTestSpec

import scala.util.Random

trait EnumerateUniqueIndexDataTypeTestAction extends BaseEnumerateDataTypesTestSpec {
  override def genIndex(dataTypes: List[ReflectedDataType], r: Random): List[Index] = {
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
}
