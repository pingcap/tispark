package org.apache.spark.sql.pushdown

import org.apache.spark.sql.catalyst.plans.BasePlanTest

/**
 * Count will be pushed down except:
 * - Count(col1,col2,...) can't be pushed down (TiDB not support)
 * - Count(set) can't be pushed down
 */
class CountPushDownSuite extends BasePlanTest() {

  private val push = Seq[String](
    "select count(id_dt) from full_data_type_table_count",
    "select count(tp_varchar) from full_data_type_table_count",
    "select count(tp_datetime) from full_data_type_table_count",
    "select count(tp_blob) from full_data_type_table_count",
    "select count(tp_binary) from full_data_type_table_count",
    "select count(tp_date) from full_data_type_table_count",
    "select count(tp_timestamp) from full_data_type_table_count",
    "select count(tp_year) from full_data_type_table_count",
    "select count(tp_bigint) from full_data_type_table_count",
    "select count(tp_decimal) from full_data_type_table_count",
    "select count(tp_double) from full_data_type_table_count",
    "select count(tp_float) from full_data_type_table_count",
    "select count(tp_int) from full_data_type_table_count",
    "select count(tp_mediumint) from full_data_type_table_count",
    "select count(tp_real) from full_data_type_table_count",
    "select count(tp_smallint) from full_data_type_table_count",
    "select count(tp_tinyint) from full_data_type_table_count",
    "select count(tp_char) from full_data_type_table_count",
    "select count(tp_nvarchar) from full_data_type_table_count",
    "select count(tp_longtext) from full_data_type_table_count",
    "select count(tp_mediumtext) from full_data_type_table_count",
    "select count(tp_text) from full_data_type_table_count",
    "select count(tp_tinytext) from full_data_type_table_count",
    "select count(tp_bit) from full_data_type_table_count",
    "select count(tp_time) from full_data_type_table_count",
    "select count(tp_enum) from full_data_type_table_count",
    "select count(*) from full_data_type_table_count",
    "select count(1) from full_data_type_table_count")

  private val notPush = Seq[String]("select count(tp_set) from full_data_type_table_count")

  test("Test - Count push down cluster") {

    tidbStmt.execute("DROP TABLE IF EXISTS `full_data_type_table_count`")
    tidbStmt.execute("""
         CREATE TABLE `full_data_type_table_count` (
        `id_dt` int(11) DEFAULT NULL,
        `tp_varchar` varchar(45) DEFAULT NULL,
        `tp_datetime` datetime DEFAULT NULL,
        `tp_blob` blob DEFAULT NULL,
        `tp_binary` binary(2) DEFAULT NULL,
        `tp_date` date DEFAULT NULL,
        `tp_timestamp` timestamp DEFAULT NULL,
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
      ) ;
      """)

    tidbStmt.execute("insert into full_data_type_table_count values()")
    tidbStmt.execute(
      "INSERT INTO `full_data_type_table_count` VALUES (-1000,'a948ddcf-9053-4700-916c-983d4af895ef','2017-11-02 00:00:00','23333','66','2017-11-02','2017-11-02 08:43:23',2017,4355836469450447576,91598606438408806444.969071534481121418,0.2054466,0.28888312,1238733782,1866044,0.09951998980446786,17438,75,'Y0FAww8awb','Y1wnS','jXatvse4l43dWcunQgGhBk4m5jyGdLWjq3DxwM6L3CWhvJAPIEP5TzIVHzE26IDHlQAu3356kyIpfArJWJHGUOScatBqERPVQn6daA7YL5zH0rr8JnRkzKhIKX5jtkSVwdeROiSM9x5faPqxdvWHq0pkKLsJjUrgoIibl6UM5eR0h8VbdbwiRYEiyp5kvnrGLs6L9xKuDYBVG1tCt9CosNLjFNZtWs0miozZomPsSI1kt3iSn4S8eVMZGlMRA9BJ1zgGBVugHPmJ6b3B7EI4wOKTbUHmg67HV4P8RXL3dsdkX0SN6O5fQUdeZS3Rx7LNRo3w2ET4EOHcRh4qEvVnWxIVMZzljy4BTwooJMYGlyZ5iVhtzhL9I9J1IeZsW5a2emi2gmzn4PqMiE3B4XG27KlMliAPbhvl3wWrxGYwu2SenibyS4fKg0qRjdTS8MOnBaGiAMYfWOZWyJNR1JkxD3q8vRCgq92FILErE9bved60Fvuv5xtH','fXPKRoPf2ETSF4I7a4eroztWS26ja6HlB8wnwgBByCaUPKCCnoyxp8Y3408mRUTn2NMPY4tf5wnqIbFmDkWEhC0EStMAjGMack24','MlDOt5EX4bnaVHn5lX8kZtSrBDNSdaRMEeh8uUcOzfVPJT493F','MSBxzzDYGOggZxDUadO9','\\0','16:43:23','1','a,b')")

    push.foreach { query =>
      val df = spark.sql(query)
      if (!extractCoprocessorRDDs(df).head.toString.contains("Aggregates")) {
        fail(
          s"count is not pushed down in query:$query,DAGRequests:" + extractCoprocessorRDDs(
            df).head.toString)
      }
      runTest(query)
    }

    notPush.foreach { query =>
      runTest(query)
    }
  }
}
