/*
 *
 * Copyright 2019 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.sql.types

import org.apache.spark.sql.{BaseTestGenerationSpec, BaseTiSparkTest}

trait BaseDataTypeTest extends BaseTiSparkTest {

  protected val generator: BaseTestGenerationSpec

  def simpleSelect(dbName: String, dataType: String): Unit = {
    setCurrentDatabase(dbName)
    val tblName = generator.getTableName(dataType)
    val query = s"select ${generator.getColumnName(dataType)} from $tblName"
    runTest(query)
  }

  def simpleSelect(dbName: String, dataType: String, desc: String): Unit = {
    setCurrentDatabase(dbName)
    val tblName = generator.getTableNameWithDesc(desc, dataType)
    val query = s"select ${generator.getColumnName(dataType)} from $tblName"
    logger.info(query)
    runTest(query)
  }

  def check(): Unit

  override def beforeAll(): Unit = {
    super.beforeAll()
    check()
  }

  init()
}
