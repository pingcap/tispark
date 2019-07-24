package org.apache.spark.sql.types

import org.apache.spark.sql.BaseTiSparkTest

trait BaseDataTypeTest extends BaseTiSparkTest {
  def simpleSelect(dbName: String, dataType: String): Unit = {
    setCurrentDatabase(dbName)
    val tblName = getTableName(dataType)
    val query = s"select ${getColumnName(dataType)} from $tblName"
    explainAndRunTest(query)
  }

  def simpleSelect(dbName: String, dataType: String, desc: String): Unit = {
    setCurrentDatabase(dbName)
    val tblName = getTableName(dataType, desc)
    val query = s"select ${getColumnName(dataType)} from $tblName"
    explainAndRunTest(query)
  }
}
