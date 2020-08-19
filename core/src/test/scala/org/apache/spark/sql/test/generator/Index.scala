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

package org.apache.spark.sql.test.generator

trait Index {
  def indexColumns: List[IndexColumn]
  val isPrimaryKey: Boolean = false
  val isUnique: Boolean = false
}

case class Key(indexColumns: List[IndexColumn]) extends Index {}

case class PrimaryKey(indexColumns: List[IndexColumn]) extends Index {
  override val isPrimaryKey: Boolean = true
}

case class UniqueKey(indexColumns: List[IndexColumn]) extends Index {
  override val isUnique: Boolean = true
}
