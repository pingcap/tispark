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

import org.apache.spark.sql.catalyst.plans.BasePlanTest

trait BasePushDownSuite extends BasePlanTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTable
    insertTable
  }

  def createTable: Unit = {
    tidbStmt.execute("DROP TABLE IF EXISTS `full_data_type_table_pk`")
    tidbStmt.execute("""
         CREATE TABLE `full_data_type_table_pk` (
        `id_dt` int(11) NOT NULL,
        `tp_varchar` varchar(45) DEFAULT NULL,
        `tp_datetime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
        PRIMARY KEY (`id_dt`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
      """)

    tidbStmt.execute("DROP TABLE IF EXISTS `full_data_type_table_no_pk`")
    tidbStmt.execute("""
         CREATE TABLE `full_data_type_table_no_pk` (
        `id_dt` int(11) DEFAULT NULL,
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
        `tp_set` set('a','b','c','d') DEFAULT NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
      """)
  }

  def insertTable: Unit = {
    tidbStmt.execute("insert into full_data_type_table_pk (id_dt) values(1)")
    tidbStmt.execute(
      "INSERT INTO `full_data_type_table_pk` VALUES (-1000,'a948ddcf-9053-4700-916c-983d4af895ef','2017-11-02 00:00:00','23333','66','2017-11-02','2017-11-02 08:43:23',2017,4355836469450447576,91598606438408806444.969071534481121418,0.2054466,0.28888312,1238733782,1866044,0.09951998980446786,17438,75,'Y0FAww8awb','Y1wnS','jXatvse4l43dWcunQgGhBk4m5jyGdLWjq3DxwM6L3CWhvJAPIEP5TzIVHzE26IDHlQAu3356kyIpfArJWJHGUOScatBqERPVQn6daA7YL5zH0rr8JnRkzKhIKX5jtkSVwdeROiSM9x5faPqxdvWHq0pkKLsJjUrgoIibl6UM5eR0h8VbdbwiRYEiyp5kvnrGLs6L9xKuDYBVG1tCt9CosNLjFNZtWs0miozZomPsSI1kt3iSn4S8eVMZGlMRA9BJ1zgGBVugHPmJ6b3B7EI4wOKTbUHmg67HV4P8RXL3dsdkX0SN6O5fQUdeZS3Rx7LNRo3w2ET4EOHcRh4qEvVnWxIVMZzljy4BTwooJMYGlyZ5iVhtzhL9I9J1IeZsW5a2emi2gmzn4PqMiE3B4XG27KlMliAPbhvl3wWrxGYwu2SenibyS4fKg0qRjdTS8MOnBaGiAMYfWOZWyJNR1JkxD3q8vRCgq92FILErE9bved60Fvuv5xtH','fXPKRoPf2ETSF4I7a4eroztWS26ja6HlB8wnwgBByCaUPKCCnoyxp8Y3408mRUTn2NMPY4tf5wnqIbFmDkWEhC0EStMAjGMack24','MlDOt5EX4bnaVHn5lX8kZtSrBDNSdaRMEeh8uUcOzfVPJT493F','MSBxzzDYGOggZxDUadO9','\\0','16:43:23','1','a,b')")
    tidbStmt.execute("insert into full_data_type_table_no_pk values()")
    tidbStmt.execute(
      "INSERT INTO `full_data_type_table_no_pk` VALUES (-1000,'a948ddcf-9053-4700-916c-983d4af895ef','2017-11-02 00:00:00','23333','66','2017-11-02','2017-11-02 08:43:23',2017,4355836469450447576,91598606438408806444.969071534481121418,0.2054466,0.28888312,1238733782,1866044,0.09951998980446786,17438,75,'Y0FAww8awb','Y1wnS','jXatvse4l43dWcunQgGhBk4m5jyGdLWjq3DxwM6L3CWhvJAPIEP5TzIVHzE26IDHlQAu3356kyIpfArJWJHGUOScatBqERPVQn6daA7YL5zH0rr8JnRkzKhIKX5jtkSVwdeROiSM9x5faPqxdvWHq0pkKLsJjUrgoIibl6UM5eR0h8VbdbwiRYEiyp5kvnrGLs6L9xKuDYBVG1tCt9CosNLjFNZtWs0miozZomPsSI1kt3iSn4S8eVMZGlMRA9BJ1zgGBVugHPmJ6b3B7EI4wOKTbUHmg67HV4P8RXL3dsdkX0SN6O5fQUdeZS3Rx7LNRo3w2ET4EOHcRh4qEvVnWxIVMZzljy4BTwooJMYGlyZ5iVhtzhL9I9J1IeZsW5a2emi2gmzn4PqMiE3B4XG27KlMliAPbhvl3wWrxGYwu2SenibyS4fKg0qRjdTS8MOnBaGiAMYfWOZWyJNR1JkxD3q8vRCgq92FILErE9bved60Fvuv5xtH','fXPKRoPf2ETSF4I7a4eroztWS26ja6HlB8wnwgBByCaUPKCCnoyxp8Y3408mRUTn2NMPY4tf5wnqIbFmDkWEhC0EStMAjGMack24','MlDOt5EX4bnaVHn5lX8kZtSrBDNSdaRMEeh8uUcOzfVPJT493F','MSBxzzDYGOggZxDUadO9','\\0','16:43:23','1','a,b')")
  }

}
