/*
 * Copyright 2020 PingCAP, Inc.
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

package org.apache.spark.sql

import com.pingcap.tikv.meta.{TiDAGRequest, TiPartitionDef}
import org.apache.spark.sql.catalyst.plans.BasePlanTest

import java.util.List

class ListPartitionPruningSuite extends BasePlanTest {

  test("list partition") {
    val table = "employees"
    tidbStmt.execute(s"DROP TABLE IF EXISTS $table")
    tidbStmt.execute(s"""
                       CREATE TABLE $table (
                       |    id INT NOT NULL,
                       |    store_id INT
                       |)
                       |PARTITION BY LIST (store_id) (
                       |    PARTITION p1 VALUES IN (1, 2, 3, 4, 5),
                       |    PARTITION p2 VALUES IN (6, 7, 8, 9, 10),
                       |    PARTITION p3 VALUES IN (11, 12, 13, 14, 15),
                       |    PARTITION p4 VALUES IN (16, 17, 18, 19, 20)
                       |);
                     """.stripMargin)

    var defs = extractDAGReqPrunedParts(spark.sql(s"select * from $table where store_id=1"))
    assert(defs.size() == 1 && defs.get(0).getName.equals("p1"))

    defs =
      extractDAGReqPrunedParts(spark.sql(s"select * from $table where store_id=5 or store_id=12"))
    assert(
      defs.size() == 2 && defs.get(0).getName.equals("p1") && defs.get(1).getName.equals("p3"))

    defs = extractDAGReqPrunedParts(
      spark.sql(s"select * from $table where store_id > 5 and store_id < 16"))
    assert(
      defs.size() == 2 && defs.get(0).getName.equals("p2") && defs.get(1).getName.equals("p3"))

    defs = extractDAGReqPrunedParts(spark.sql(s"select * from $table where store_id = 21"))
    assert(defs.size() == 0)
  }

  test("list partition with year function") {
    val table = "list_year"
    tidbStmt.execute(s"DROP TABLE IF EXISTS $table")
    tidbStmt.execute(s"""
                       CREATE TABLE $table (
                        |    id INT NOT NULL,
                        |    date datetime
                        |)
                        |PARTITION BY LIST (year(date)) (
                        |    PARTITION p1 VALUES IN (1990, 1991),
                        |    PARTITION p2 VALUES IN (2000, 2001),
                        |    PARTITION p3 VALUES IN (2020, 2021,2022),
                        |    PARTITION p4 VALUES IN (2023)
                        |);
                     """.stripMargin)

    println(s"""
                       CREATE TABLE $table (
               |    id INT NOT NULL,
               |    date datetime
               |)
               |PARTITION BY LIST (year(date)) (
               |    PARTITION p1 VALUES IN (1990, 1991),
               |    PARTITION p2 VALUES IN (2000, 2001),
               |    PARTITION p3 VALUES IN (2020, 2021,2022),
               |    PARTITION p4 VALUES IN (2023)
               |);
                     """.stripMargin)

    var defs =
      extractDAGReqPrunedParts(spark.sql(s"select * from $table where date='1990-01-01'"))
    assert(defs.size() == 1 && defs.get(0).getName.equals("p1"))

    defs = extractDAGReqPrunedParts(
      spark.sql(s"select * from $table where date='1991-01-01' or date='2020-01-01'"))
    assert(
      defs.size() == 2 && defs.get(0).getName.equals("p1") && defs.get(1).getName.equals("p3"))

    defs = extractDAGReqPrunedParts(
      spark.sql(s"select * from $table where date > '1991-01-01' and date < '2023-01-02'"))
    assert(
      defs.size() == 2 && defs.get(0).getName.equals("p2") && defs.get(1).getName.equals("p3"))

    defs = extractDAGReqPrunedParts(spark.sql(s"select * from $table where date = '1888-01-01'"))
    assert(defs.size() == 0)
  }

  test("list column partition with mutil column") {
    val table = "mutil_column"
    tidbStmt.execute(s"DROP TABLE IF EXISTS $table")
    tidbStmt.execute(s"""
                       CREATE TABLE $table (
                         |    ID int,
                         |    NAME varchar(10)
                         |)
                         |PARTITION BY LIST COLUMNS(ID,NAME) (
                         |     partition p0 values IN ((1,'a'),(2,'b')),
                         |     partition p1 values IN ((3,'c'),(4,'d')),
                         |     partition p2 values IN ((5,'e'),(6,'f'))
                         |);
                     """.stripMargin)

    var defs = extractDAGReqPrunedParts(spark.sql(s"select * from $table where id=3"))
    assert(defs.size() == 1 && defs.get(0).getName.equals("p1"))

    defs = extractDAGReqPrunedParts(spark.sql(s"select * from $table where name = 'a'"))
    assert(defs.size() == 1 && defs.get(0).getName.equals("p0"))

    defs = extractDAGReqPrunedParts(spark.sql(s"select * from $table where id > 2"))
    assert(
      defs.size() == 2 && defs.get(0).getName.equals("p1") && defs.get(1).getName.equals("p2"))

    defs = extractDAGReqPrunedParts(spark.sql(s"select * from $table where name > 'c'"))
    assert(
      defs.size() == 2 && defs.get(0).getName.equals("p1") && defs.get(1).getName.equals("p2"))

    defs = extractDAGReqPrunedParts(spark.sql(s"select * from $table where id > 2 and name >'d'"))
    assert(defs.size() == 1 && defs.get(0).getName.equals("p2"))

    defs = extractDAGReqPrunedParts(spark.sql(s"select * from $table where id > 2 and name <'e'"))
    assert(defs.size() == 1 && defs.get(0).getName.equals("p1"))

    defs = extractDAGReqPrunedParts(spark.sql(s"select * from $table where id > 1 and name <'e'"))
    assert(
      defs.size() == 2 && defs.get(0).getName.equals("p0") && defs.get(1).getName.equals("p1"))

    defs =
      extractDAGReqPrunedParts(spark.sql(s"select * from $table where id > 2 and name = 'a'"))
    assert(defs.size() == 0)

  }

  private def extractDAGReqPrunedParts(df: DataFrame): List[TiPartitionDef] = {
    val dag = extractDAGRequests(df)
    if (dag.isEmpty) {
      return new java.util.ArrayList[TiPartitionDef]()
    }
    dag.head.getPrunedParts
  }

}
