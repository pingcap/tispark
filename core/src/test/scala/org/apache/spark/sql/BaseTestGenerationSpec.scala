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

  protected val rowCount: Int

  protected val preDescription: String = "Generating Data for "

  def getTableName(dataTypes: String*): String

  def getTableNameWithDesc(desc: String, dataTypes: String*): String

  def getColumnName(dataType: String): String = s"col_$dataType"

  def getIndexName(dataTypes: String*): String =
    s"idx_${dataTypes.map(getColumnName).mkString("_")}"

}
