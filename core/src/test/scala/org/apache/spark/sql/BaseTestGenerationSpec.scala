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

package org.apache.spark.sql

trait BaseTestGenerationSpec {

  protected val rowCount: Int = 50

  private def toString(dataTypes: Seq[String]) = dataTypes.map(_.head).mkString("_")

  protected def getTableName(dataTypes: String*): String = s"test_${toString(dataTypes)}"

  protected def getTableNameWithDesc(desc: String, dataTypes: String*): String =
    s"test_${desc}_${toString(dataTypes)}"

  protected def getColumnName(dataType: String): String = s"col_$dataType"

  protected def getIndexName(dataTypes: String*): String = s"idx_${toString(dataTypes)}"

}
