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

package org.apache.spark.sql.pushdown

/**
 * support type (MYSQLTYPE):smallint,bigint,decimal,mediumint,real(double),tinyint,int,double,year

 * unsupported type: char,float,datatime,varchar,timestamp
 * because Spark cast them to double,cast can't be pushed down to tikv
 *
 * This test will
 * 1. check whether sum is pushed down
 * 2. check whether the result is right(equals to spark jdbc or equals to tidb)
 */
class SumPushDownSuite extends BasePushDownSuite {

  private val allCases = Seq[String](
<<<<<<< HEAD
    "select sum(tp_smallint) from full_data_type_table_cluster",
    "select sum(tp_bigint) from full_data_type_table_cluster",
    "select sum(tp_decimal) from full_data_type_table_cluster",
    "select sum(tp_mediumint) from full_data_type_table_cluster",
    "select sum(tp_real) from full_data_type_table_cluster",
    "select sum(tp_tinyint) from full_data_type_table_cluster",
    "select sum(id_dt) from full_data_type_table_cluster",
    "select sum(tp_int) from full_data_type_table_cluster",
    "select sum(tp_double) from full_data_type_table_cluster")

  test("Test - Sum push down") {
    tidbStmt.execute("DROP TABLE IF EXISTS `full_data_type_table_cluster`")
    tidbStmt.execute("""
         CREATE TABLE `full_data_type_table_cluster` (
        `id_dt` int(11) NOT NULL,
        `tp_varchar` varchar(45) DEFAULT NULL,
        `tp_datetime` datetime DEFAULT CURRENT_TIMESTAMP,
        `tp_blob` blob DEFAULT NULL,
        `tp_binary` binary(2) DEFAULT NULL,
        `tp_date` date DEFAULT NULL,
        `tp_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `tp_year` year DEFAULT NULL,
        `tp_bigint` bigint(20) DEFAULT NULL,
        `tp_decimal` decimal(38,18) DEFAULT NULL,
        `tp_double` double DEFAULT NULL,
        `tp_float` float DEFAULT NULL,
        `tp_int` int(11) DEFAULT NULL,
        `tp_mediumint` mediumint(9) DEFAULT NULL,
        `tp_real` double DEFAULT NULL,
        `tp_smallint` smallint(6) DEFAULT NULL,
        `tp_tinyint` tinyint(4) DEFAULT NULL,
        `tp_char` char(10) DEFAULT NULL,
        `tp_nvarchar` varchar(40) DEFAULT NULL,
        `tp_longtext` longtext DEFAULT NULL,
        `tp_mediumtext` mediumtext DEFAULT NULL,
        `tp_text` text DEFAULT NULL,
        `tp_tinytext` tinytext DEFAULT NULL,
        `tp_bit` bit(1) DEFAULT NULL,
        `tp_time` time DEFAULT NULL,
        `tp_enum` enum('1','2','3','4') DEFAULT NULL,
        `tp_set` set('a','b','c','d') DEFAULT NULL,
        PRIMARY KEY (`id_dt`)/*T![clustered_index] CLUSTERED */
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
      """)
=======
    "select sum(tp_smallint) from ",
    "select sum(tp_bigint) from ",
    "select sum(tp_decimal) from ",
    "select sum(tp_mediumint) from ",
    "select sum(tp_real) from ",
    "select sum(tp_tinyint) from ",
    "select sum(id_dt) from ",
    "select sum(tp_int) from ",
    "select sum(tp_double) from ")
>>>>>>> 803fc5943 (Fix count/avg push down (#2445))

  test("Test - Sum push down with pk") {
    val tableName = "full_data_type_table_pk"
    allCases.foreach { query =>
      val sql = query + tableName
      val df = spark.sql(sql)
      if (!extractCoprocessorRDDs(df).head.toString.contains("Aggregates")) {
        fail(
          s"sum is not pushed down in query:$query,DAGRequests:" + extractCoprocessorRDDs(
            df).head.toString)
      }
      runTest(sql)
    }
  }

<<<<<<< HEAD
=======
  test("Test - Sum push down no pk") {
    val tableName = "full_data_type_table_no_pk"
    allCases.foreach { query =>
      val sql = query + tableName
      val df = spark.sql(sql)
      if (!extractCoprocessorRDDs(df).head.toString.contains("Aggregates")) {
        fail(
          s"sum is not pushed down in query:$query,DAGRequests:" + extractCoprocessorRDDs(
            df).head.toString)
      }
      runTest(sql)
    }
  }

>>>>>>> 803fc5943 (Fix count/avg push down (#2445))
}
