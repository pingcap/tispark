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

package com.pingcap.tispark.tls

import com.pingcap.tispark.telemetry.TiSparkTeleInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{BaseTiSparkTest, Row}

class TiSparkTLSSuite extends BaseTiSparkTest {

  val TLSEnable: Boolean = CheckTLSEnable.isEnableTest

  override def beforeAll(): Unit = {
    if (!TLSEnable) {
      return
    }
    super.beforeAll()
    tidbStmt.execute("CREATE DATABASE IF NOT EXISTS `TLS_TEST`;")
    tidbStmt.execute(
      "CREATE TABLE IF NOT EXISTS `TLS_TEST`.`tls_test_table`(id int, name varchar (128)); ")
    tidbStmt.execute("INSERT INTO `TLS_TEST`.`tls_test_table` VALUES (1, 'TiDB');")
  }

  override def afterAll(): Unit = {
    if (!TLSEnable) {
      return
    }
    tidbStmt.execute("DROP DATABASE IF EXISTS `TLS_TEST`")
    super.afterAll()
  }

  test("test Spark SELECT SQL by SSL connection") {
    if (!TLSEnable) {
      cancel
    }
    val df = spark.sql("SELECT * FROM `TLS_TEST`.`tls_test_table`")
    assert(1 == df.collect().head.get(0))
    assert("TiDB".equals(df.collect().head.get(1)))
  }

  test("test Spark DELETE SQL by SSL connection") {
    if (!TLSEnable) {
      cancel
    }
    spark.sql("DELETE FROM `TLS_TEST`.`tls_test_table` WHERE `id` = 1")
    assert(0 == spark.sql("SELECT * FROM `TLS_TEST`.`tls_test_table`").collect().size)
  }

  test("test Spark WRITE SQL by SSL connection") {
    if (!TLSEnable) {
      cancel
    }
    val row1 = Row(2, "TiKV")
    val row2 = Row(3, "PD")
    val schema =
      StructType(List(StructField("id", IntegerType), StructField("name", StringType)))
    val data: RDD[Row] = sc.makeRDD(List(row1, row2))
    var df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", "TLS_TEST")
      .option("table", "tls_test_table")
      .option("tikv.tls_enable", "true")
      .option("jdbc.tls_enable", "true")
      .option("jdbc.server_cert_store", "file:/config/cert/jks/server-cert-store")
      .option("jdbc.server_cert_password", "12345678")
      .option("jdbc.client_cert_store", "file:/config/cert/jks/client-keystore")
      .option("jdbc.client_cert_password", "123456")
      .mode("append")
      .save()
    df = spark.sql("SELECT * FROM `TLS_TEST`.`tls_test_table`")
    assert(2 == df.collect().size)
    assert(2 == df.collect().head.get(0))
    assert("TiKV".equals(df.collect().head.get(1)))
  }

  test("test get TiDB version with HTTPS") {
    val tiSparkTeleInfo = TiSparkTeleInfo.getTiSparkTeleInfo()
    assert(!tiSparkTeleInfo.get("tidb_version").contains("UNKNOWN"))
  }
}
