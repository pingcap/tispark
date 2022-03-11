/*
 *
 * Copyright 2019 PingCAP Inc.
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

package com.pingcap.tispark.test.generator

import com.pingcap.tispark.test.generator.DataType._
import org.scalatest.FunSuite

import scala.util.Random

class DataGeneratorSuite extends FunSuite {

  test("base test for schema generator") {
    val r = new Random(1234)
    val schema = DataGenerator.schemaGenerator(
      "tispark_test",
      "test_table",
      r,
      List(
        (INT, "", "not null primary key"),
        (INT, "", "default null"),
        (DOUBLE, "", "not null default 0.2"),
        (VARCHAR, "50", "default null"),
        (DECIMAL, "20,3", "default null")),
      List(
        Key(List(DefaultColumn(2), DefaultColumn(3))),
        Key(List(PrefixColumn(4, 20))),
        Key(List(DefaultColumn(3), DefaultColumn(5)))))
    val schema2 = DataGenerator.schemaGenerator(
      "tispark_test",
      "test_table",
      r,
      List(
        (INT, "", "not null"),
        (INT, "", "default null"),
        (DOUBLE, "", "not null default 0.2"),
        (VARCHAR, "50", "default null"),
        (DECIMAL, "20,3", "default null")),
      List(
        PrimaryKey(List(DefaultColumn(1))),
        Key(List(DefaultColumn(2), DefaultColumn(3))),
        Key(List(PrefixColumn(4, 20))),
        Key(List(DefaultColumn(3), DefaultColumn(5)))))
    val answer =
      """CREATE TABLE `tispark_test`.`test_table` (
        |  `col_int0` int(11) not null,
        |  `col_int1` int(11) default null,
        |  `col_double` double not null default 0.2,
        |  `col_varchar` varchar(50) default null,
        |  `col_decimal` decimal(20,3) default null,
        |  PRIMARY KEY (`col_int0`),
        |  KEY `idx_col_int1_col_double`(`col_int1`,`col_double`),
        |  KEY `idx_col_varchar`(`col_varchar`(20)),
        |  KEY `idx_col_double_col_decimal`(`col_double`,`col_decimal`)
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""".stripMargin
    assert(schema.toString === answer)
    assert(schema2.toString === answer)
  }

  test("test generate schema") {
    val r = new Random(1234)
    val schema = DataGenerator.schemaGenerator(
      "tispark_test",
      "test_table",
      r,
      List(
        (INT, "", "not null primary key"),
        (INT, "", "default null"),
        (BIT, "3", "default null"),
        (DOUBLE, "", "not null default 0.2"),
        (VARCHAR, "50", "default null"),
        (DECIMAL, "10,3", "default null")),
      List(
        Key(List(DefaultColumn(2), DefaultColumn(4))),
        Key(List(PrefixColumn(5, 20))),
        Key(List(DefaultColumn(4), DefaultColumn(5)))))
    val answer =
      """CREATE DATABASE IF NOT EXISTS `tispark_test`;
        |USE `tispark_test`;
        |DROP TABLE IF EXISTS `test_table`;
        |CREATE TABLE `tispark_test`.`test_table` (
        |  `col_int0` int(11) not null,
        |  `col_int1` int(11) default null,
        |  `col_bit` bit(3) default null,
        |  `col_double` double not null default 0.2,
        |  `col_varchar` varchar(50) default null,
        |  `col_decimal` decimal(10,3) default null,
        |  PRIMARY KEY (`col_int0`),
        |  KEY `idx_col_int1_col_double`(`col_int1`,`col_double`),
        |  KEY `idx_col_varchar`(`col_varchar`(20)),
        |  KEY `idx_col_double_col_varchar`(`col_double`,`col_varchar`)
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
        |INSERT INTO `test_table` VALUES (-1517918040,-1933932397,b'101',0.14658670415052433,'UM4yfPW0uW6vhd4xZhf85SapkNVdldOjxbKYVH2HEKNScUZ1wd',-1212674.048);
        |INSERT INTO `test_table` VALUES (1115789266,1442904595,b'000',0.3708618354358446,'Sr7a7MFnMo0mrcSivSKlsY4jUcJLbK6LOEBuSpUErRfal80XMx',765718.528);
        |INSERT INTO `test_table` VALUES (-208917030,397902075,b'000',0.038461224518263615,'BRBTybDDKVrLb7JzuW5w3tRdIj2Yna2RwK0WzLuG4bVjD4eN4B',3583815.68);
        |INSERT INTO `test_table` VALUES (1019800440,875635521,b'100',0.5205586068847993,'y6mXgMmgKjbCwoMvrczYuW3lUmchoiRsCGQPZlnrgio2tZ4qFY',8848781.824);
        |INSERT INTO `test_table` VALUES (-611652875,64950077,b'001',0.897234285735065,'t3a9El54AREgcJ0ItKSVxYDDqQlzWtu7ww4kHlLbbwN0jOc3Qn',1889909.76);
        |INSERT INTO `test_table` VALUES (1362132786,1489956094,b'001',0.756826923315635,'CG06scsyHIucx8CfS2YoHrLg0hLnNQwlRsKhXEXxWhWxObRAPv',-6272569.856);
        |INSERT INTO `test_table` VALUES (1968097058,1957992572,b'010',0.38957083495431966,'5py0joCZHRb1TipgRDnu5nr5onHC9XFjYQ1h1Jn1VLL11KExt1',1392960.512);
        |INSERT INTO `test_table` VALUES (-1,-1,b'011',0.16390611322459636,'VHTkkI6ne06S7rKvw39kA3QLvFjX9akhExnnnOSVeevJuuKGAH',-4457936.896);""".stripMargin
    val data: SchemaAndData = DataGenerator.randomDataGenerator(schema, 10, "tispark-test", r)
    assert(answer.equals(data.getInitSQLList.mkString("\n")))
  }
}
