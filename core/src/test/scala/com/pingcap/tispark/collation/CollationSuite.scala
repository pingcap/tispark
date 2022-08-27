/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.collation

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.BaseTiSparkTest
import org.scalatest.Matchers.{convertToAnyShouldWrapper, have, the}

import scala.util.Random

class CollationSuite extends BaseTiSparkTest {

  private def generateRandomString(length: Long): String = {
    val alphaNum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    val s = StringBuilder.newBuilder
    for (_ <- 0L until length) {
      s.append(alphaNum.charAt(Math.abs(Random.nextInt(10000)) % alphaNum.length))
    }
    s.mkString
  }

  test("utf8mb4_bin, utf8mb4_general_ci and utf8mb4_unicode_ci with clustered index test") {
    val collations = Array("utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci")
    for (collation <- collations) {
      tidbStmt.execute(s"""
           |    DROP TABLE IF EXISTS `tispark_test`.`collation_test_table`;
           |    CREATE TABLE `tispark_test`.`collation_test_table` (
           |      `col_bit` bit(1) not null,
           |      `col_varchar` varchar(23) not null,
           |      `col_int0` int(11) not null,
           |      `col_int1` int(11) not null,
           |      UNIQUE KEY (`col_int0`),
           |      PRIMARY KEY (`col_varchar`(5),`col_bit`) /*T![clustered_index] CLUSTERED */
           |    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=${collation}
           |""".stripMargin)

      val col_varchar1 = generateRandomString(23)
      val col_varchar2 = generateRandomString(23)

      spark.sql(s"""
           |  INSERT INTO `tispark_test`.`collation_test_table` VALUES (0, '${col_varchar1}',-1176927076,-199700133);
           |""".stripMargin)

      spark.sql(s"""
           |  INSERT INTO `tispark_test`.`collation_test_table` VALUES (1, '${col_varchar2}',-1908012631,586989409);
           |""".stripMargin)

      val df =
        spark.sql("select * from `tispark_test`.`collation_test_table` order by `col_bit` desc")
      assert(df.count() == 2);
      assert(df.head().getString(1) == col_varchar2)
    }
  }

  test(
    "utf8mb4_bin, utf8mb4_general_ci and utf8mb4_unicode_ci with special clustered index test") {
    val collations = Array("utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci")
    val col_varchars = Array(
      "a        ",
      "😜😃",
      "åß∂ƒ©˙∆˚¬…æ",
      "ЁЂЃЄЅІЇЈЉЊЋЌЍЎЏАБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдежзийклмнопрстуфхцчшщъыьэюя")
    for (collation <- collations) {
      tidbStmt.execute(s"""
           |    DROP TABLE IF EXISTS `tispark_test`.`collation_test_table`;
           |    CREATE TABLE `tispark_test`.`collation_test_table` (
           |      `col_varchar` varchar(256) not null,
           |      `col_int0` int(11) not null,
           |      `col_int1` int(11) not null,
           |      UNIQUE KEY (`col_int0`),
           |      PRIMARY KEY (`col_varchar`(4)) /*T![clustered_index] CLUSTERED */
           |    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=${collation}
           |""".stripMargin)

      for (i <- 0 until col_varchars.length) {
        spark.sql(s"""
             |  INSERT INTO `tispark_test`.`collation_test_table` VALUES ('${col_varchars(i)}',${i},-199700133);
             |""".stripMargin)
      }
      val df = spark.sql("select * from `tispark_test`.`collation_test_table`")
      df.collect()
        .foreach(row => {
          assert(row.getString(0) == col_varchars(row.getLong(1).toInt))
        })
    }
  }

  test("utf8mb4_bin, utf8mb4_general_ci and utf8mb4_unicode_ci compare test") {
    val collations = Array("utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci")
    for (collation <- collations) {
      tidbStmt.execute(s"""
           |   DROP TABLE IF EXISTS `tispark_test`.`collation_test_table`;
           |   CREATE TABLE `tispark_test`.`collation_test_table` (
           |    `col_varchar` varchar(2) not null
           |    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=${collation}
           |  """.stripMargin)

      spark.sql(
        """
          |INSERT INTO `tispark_test`.`collation_test_table` VALUES ('Aa'),('AA'),('aa'),('aA'),('Bb'),('BB'),('bb'),('bB');
          |""".stripMargin)

      val df1 =
        spark.sql("SELECT * FROM `tispark_test`.`collation_test_table` WHERE col_varchar > 'a'")
      if (collation == "utf8mb4_bin") {
        assert(df1.count() == 4)
      } else if (collation == "utf8mb4_general_ci") {
        assert(df1.count() == 8)
      } else if (collation == "utf8mb4_unicode_ci") {
        assert(df1.count() == 8)
      }
      val df2 = spark.sql(
        "SELECT * FROM `tispark_test`.`collation_test_table` WHERE col_varchar like 'aa'")
      if (collation == "utf8mb4_bin") {
        assert(df2.count() == 1)
      } else if (collation == "utf8mb4_general_ci") {
        assert(df2.count() == 4)
      } else if (collation == "utf8mb4_unicode_ci") {
        assert(df2.count() == 4)
      }
    }

  }

  test("utf8mb4_general_ci and utf8mb4_unicode_ci with primary index conflict test") {
    val collations = Array("utf8mb4_general_ci", "utf8mb4_unicode_ci")
    for (collation <- collations) {
      tidbStmt.execute(s"""
           |   DROP TABLE IF EXISTS `tispark_test`.`collation_test_table`;
           |   CREATE TABLE `tispark_test`.`collation_test_table` (
           |    `col_varchar` varchar(2) not null,
           |    PRIMARY KEY (`col_varchar`)
           |    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=${collation}
           |  """.stripMargin)

      the[TiBatchWriteException] thrownBy {
        spark.sql("""
            |INSERT INTO `tispark_test`.`collation_test_table` VALUES ('Aa'),('AA');
            |""".stripMargin)
      }
    } should have message "duplicate unique key or primary key"
  }
}
