package org.apache.spark.sql.types.pk

import org.apache.spark.sql.BaseTiSparkTest
import org.apache.spark.sql.test.generator.DataType.ReflectedDataType

case class MultiColumnDataTypePKGenerator(dataTypes: List[ReflectedDataType],
                                          unsignedDataTypes: List[ReflectedDataType],
                                          dataTypeTestDir: String,
                                          database: String,
                                          testDesc: String)
    extends BaseTiSparkTest
    with GenerateMultiColumnPKDataTypeTestAction {

  def loadTestData(tableName: String): Unit = {
    logger.info(s"${preDescription}Test $tableName - $testDesc")
    loadSQLFile(dataTypeTestDir, tableName, checkTiFlashReplica = canTestTiFlash)
  }
}
