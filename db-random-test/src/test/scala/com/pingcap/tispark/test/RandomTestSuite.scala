/*
 *
 * Copyright 2021 PingCAP, Inc.
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

package com.pingcap.tispark.test

import com.pingcap.tispark.test.generator.DataType.INT
import com.pingcap.tispark.test.generator.NullableType
import org.scalatest.FunSuite

import scala.util.Random

class RandomTestSuite extends FunSuite with RandomTest {
  override protected val r: Random = new Random(1234)

  private val rowCount = 1

  private val database = "random_test"

  test("random test") {
    val schemaAndDataList = genSchemaAndData(
      rowCount,
      List(INT, INT).map(d => genDescription(d, NullableType.NumericNotNullable)),
      database,
      isClusteredIndex = true)

    assert(1 == schemaAndDataList.size)
    val schemaAndData = schemaAndDataList.head

    assert(rowCount == schemaAndData.data.size)

    val answer = """CREATE DATABASE IF NOT EXISTS `random_test`;
                   |USE `random_test`;
                   |DROP TABLE IF EXISTS `test_351626634_1517918040`;
                   |CREATE TABLE `random_test`.`test_351626634_1517918040` (
                   |  `col_int0` int(11) not null,
                   |  `col_int1` int(11) not null
                   |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
                   |INSERT INTO `test_351626634_1517918040` VALUES (-1,-1);""".stripMargin
    println(schemaAndData.getInitSQLList.mkString("\n"))
    assert(answer.equals(schemaAndData.getInitSQLList.mkString("\n")))
  }
}
