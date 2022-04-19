/*
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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.tls

import org.apache.spark.sql.BaseTiSparkTest

class TiSparkTLSSuite extends BaseTiSparkTest{

  override def beforeAll(): Unit = {
    super.beforeAll()
    tidbStmt.execute("CREATE DATABASE IF NOT EXISTS `TLS_TEST`;")
    tidbStmt.execute("CREATE TABLE IF NOT EXISTS `TLS_TEST`.`tls_test_table`(id int, name varchar (128)); ")
    tidbStmt.execute("INSERT INTO `TLS_TEST`.`tls_test_table` VALUES (1, 'jack');")
  }
  override def afterAll(): Unit = {
    tidbStmt.execute("DROP TABLE IF EXISTS `TLS_TEST`.`tls_test_table`")
    super.afterAll()
  }

  test ("test Spark SQL by SSL connection") {
    assert(1 == spark.sql("SELECT * FROM `TLS_TEST`.`tls_test_table`").collect().head.get(0))
    assert("jack".equals(spark.sql("SELECT * FROM `TLS_TEST`.`tls_test_table`").collect().head.get(1)))
  }
}
