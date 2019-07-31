package org.apache.spark.sql.types.pk

import org.apache.spark.sql.BaseTiSparkTest
import org.apache.spark.sql.test.generator.DataType.{getTypeName, ReflectedDataType}

case class MultiColumnDataTypePKGenerator(dataTypes: List[ReflectedDataType],
                                          unsignedDataTypes: List[ReflectedDataType],
                                          dataTypeTestDir: String,
                                          database: String,
                                          testDesc: String)
    extends BaseTiSparkTest
    with GenerateMultiColumnPKDataTypeTestAction {
  def getColumnName(offset: Int): String = {
    val dataType = dataTypes(offset)
    if (dataTypes.count(_ == dataType) > 1) {
      var cnt = 0
      for (i <- 0 until offset) {
        if (dataTypes(i) == dataType) {
          cnt += 1
        }
      }
      s"${super.getColumnName(dataType.toString)}$cnt"
    } else {
      super.getColumnName(dataType.toString)
    }
  }
  def loadTestData(dataTypes: List[ReflectedDataType]): Unit = {
    for (i <- 5 until 7) { //dataTypes.indices) {
      for (j <- 5 until 7) { //dataTypes.indices) {
        val dt = List(dataTypes(i), dataTypes(j)) ++ dataTypes
        val tableName = getTableName(dt.map(getTypeName): _*)
        logger.info(s"${preDescription}Test $tableName - $testDesc")
        loadSQLFile(dataTypeTestDir, tableName)
      }
    }

  }
}
