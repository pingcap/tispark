package org.apache.spark.sql.insertion

import org.apache.spark.sql.test.generator.DataType.ReflectedDataType
import org.apache.spark.sql.test.generator.TestDataGenerator.isStringType
import org.apache.spark.sql.test.generator.{DefaultColumn, Index, PrefixColumn, PrimaryKey}

import scala.util.Random

trait EnumeratePKDataTypeTestAction extends BaseEnumerateDataTypesTestSpec {
  override def genIndex(dataTypes: List[ReflectedDataType], r: Random): List[Index] = {
    val size = dataTypes.length
    val keyList = scala.collection.mutable.ListBuffer.empty[PrimaryKey]
    for (i <- 0 until size) {
      // we add extra one to the column id since 1 is reserved to primary key
      val pkCol = if (isStringType(dataTypes(i))) {
        PrefixColumn(i + 1, r.nextInt(4) + 2) :: Nil
      } else {
        DefaultColumn(i + 1) :: Nil
      }
      keyList += PrimaryKey(pkCol)
    }
    keyList.toList
  }
}
