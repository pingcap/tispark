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

import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.Matchers.{be, noException}

class JDBCTLSSuite extends SharedSQLContext {

  override def beforeAll(): Unit = {
    conf.set("enableJDBCSSL", "true")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    tidbStmt.execute(s"DROP DATABASE IF EXISTS `TLS_TEST`;")
    super.afterAll();
  }

  test("test JDBC driver connect") {
    // do nothing, because JDBC connector init in beforeAll()
    noException should be thrownBy ()
  }

  test("test JDBC connection is SSL") {
    val result = tidbStmt.executeQuery("SHOW STATUS LIKE \"%Ssl_cipher%\";")
    while (result.next()) {
      if (result.getString("Variable_name").equals("Ssl_cipher")) {
        assert(!result.getString("Value").equals(""))
      }
    }
  }

  test("test JDBC func") {
    tidbStmt.execute("CREATE DATABASE IF NOT EXISTS `TLS_TEST`;")
    tidbStmt.execute(
      "CREATE TABLE IF NOT EXISTS `TLS_TEST`.`tls_test_table`(id int, name varchar (128)); ")
    tidbStmt.execute("INSERT INTO `TLS_TEST`.`tls_test_table` VALUES (1, 'jack');")
    val result = tidbStmt.executeQuery("SELECT * FROM `TLS_TEST`.`tls_test_table`")
    while (result.next()) {
      assert(result.getInt("id").equals(1))
      assert(result.getString("name").equals("jack"))
    }
  }
}
