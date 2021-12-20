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
        "GRANT 'app_write'@'%' TO 'dev1'@'%'" :: Nil
    )
    result.globalPriv should equal(
      List(MySQLPriv.CreateTablespacePriv, MySQLPriv.SelectPriv)
    )
    result.databasePrivs should equal(
      Map(
        "mysql" -> List(MySQLPriv.CreatePriv, MySQLPriv.UpdatePriv),
        "tpch_test" -> List(
          MySQLPriv.InsertPriv,
          MySQLPriv.UpdatePriv,
          MySQLPriv.DeletePriv
        )
      )
    )
    result.tablePrivs should equal(
      Map(
        "test.table" -> List(MySQLPriv.DeletePriv),
        "test.user" -> List(MySQLPriv.DeletePriv)
      )
    )
  }

  test("Extract role from jdbc result should be correct") {
    val roles: List[String] = TiAuthorization.extractRoles(
      "GRANT DELETE ON test.user TO 'root'@'%' WITH GRANT OPTION" ::
        "GRANT DELETE ON test.table TO 'root'@'%' WITH GRANT OPTION" ::
        "GRANT CREATE,UPDATE ON mysql.* TO 'root'@'%' WITH GRANT OPTION" ::
        "GRANT CREATE TABLESPACE,SELECT ON *.* TO 'root'@'%' WITH GRANT OPTION" ::
        "GRANT INSERT,UPDATE,DELETE ON tpch_test.* TO 'root'@'%'" ::
        "GRANT 'app_read'@'%','app_write'@'%' TO 'rw_user1'@'localhost'" :: Nil
    )
    roles should equal(List("app_read", "app_write"))
  }
}
