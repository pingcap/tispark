/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.partition

import org.apache.spark.sql.BaseTiSparkTest
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

import java.sql.ResultSet

class PartitionBaseSuite extends BaseTiSparkTest{

  val table: String = "test_partition_write"
  val database: String = "tispark_test"

  override def beforeEach(): Unit = {
    super.beforeEach()
    tidbStmt.execute(s"drop table if exists `$database`.`$table`")
  }

  protected def checkPartitionJDBCResult(expected: Map[String, Array[Array[Any]]]) = {
    for ((partition, result) <- expected) {
      val insertResultJDBC =
        tidbStmt.executeQuery(s"select * from `$database`.`$table` partition(${partition})")
      checkJDBCResult(insertResultJDBC, result)
    }
  }

  def checkJDBCResult(resultJDBC: ResultSet, rows: Array[Array[Any]]): Unit = {
    val rsMetaData = resultJDBC.getMetaData
    var sqlData: Seq[Seq[AnyRef]] = Seq()
    while (resultJDBC.next()) {
      var row: Seq[AnyRef] = Seq()
      for (i <- 1 to rsMetaData.getColumnCount) {
        resultJDBC.getObject(i) match {
          case x: Array[Byte] => row = row :+ new String(x)
          case _ => row = row :+ resultJDBC.getObject(i)
        }
      }
      sqlData = sqlData :+ row
    }
    sqlData should contain theSameElementsAs rows
  }
}
