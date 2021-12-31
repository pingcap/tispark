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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.auth

import com.pingcap.tispark.UnitSuite

class TiAuthSuite extends UnitSuite {

  test("parse privilege from jdbc result should be correct") {
    val result = TiAuthorization.parsePrivilegeFromRow(
      "GRANT DELETE ON test.user TO 'root'@'%' WITH GRANT OPTION" ::
        "GRANT DELETE ON test.table TO 'root'@'%' WITH GRANT OPTION" ::
        "GRANT CREATE,UPDATE ON mysql.* TO 'root'@'%' WITH GRANT OPTION" ::
        "GRANT CREATE TABLESPACE,SELECT ON *.* TO 'root'@'%' WITH GRANT OPTION" ::
        "GRANT INSERT,UPDATE,DELETE ON tpch_test.* TO 'root'@'%'" ::
        "GRANT 'app_write'@'%' TO 'dev1'@'%'" :: Nil)
    result.globalPriv should equal(List(MySQLPriv.CreateTablespacePriv, MySQLPriv.SelectPriv))
    result.databasePrivs should equal(
      Map(
        "mysql" -> List(MySQLPriv.CreatePriv, MySQLPriv.UpdatePriv),
        "tpch_test" -> List(MySQLPriv.InsertPriv, MySQLPriv.UpdatePriv, MySQLPriv.DeletePriv)))
    result.tablePrivs should equal(Map(
      "test" -> Map("table" -> List(MySQLPriv.DeletePriv), "user" -> List(MySQLPriv.DeletePriv))))
  }

  test("Extract role from jdbc result should be correct") {
    val roles: List[String] = TiAuthorization.extractRoles(
      "GRANT DELETE ON test.user TO 'root'@'%' WITH GRANT OPTION" ::
        "GRANT DELETE ON test.table TO 'root'@'%' WITH GRANT OPTION" ::
        "GRANT CREATE,UPDATE ON mysql.* TO 'root'@'%' WITH GRANT OPTION" ::
        "GRANT CREATE TABLESPACE,SELECT ON *.* TO 'root'@'%' WITH GRANT OPTION" ::
        "GRANT INSERT,UPDATE,DELETE ON tpch_test.* TO 'root'@'%'" ::
        "GRANT 'app_read'@'%','app_write'@'%' TO 'rw_user1'@'localhost'" :: Nil)
    roles should equal(List("app_read", "app_write"))
  }
}
