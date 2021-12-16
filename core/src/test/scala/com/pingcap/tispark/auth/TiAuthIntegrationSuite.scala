package com.pingcap.tispark.auth

import com.pingcap.tispark.datasource.BaseBatchWriteWithoutDropTableTest
import org.scalatest.Matchers.{an, be, noException}

import java.sql.SQLException

class TiAuthIntegrationSuite
  extends BaseBatchWriteWithoutDropTableTest("test_auth_basic") {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sqlContext.setConf("tidb.addr", "127.0.0.1")
    spark.sqlContext.setConf("tidb.port", "4000")
    spark.sqlContext.setConf("tidb.user", "tispark_unit_test_user")
    spark.sqlContext.setConf("tidb.password", "")

    jdbcUpdate("CREATE ROLE 'test_read', 'test_write'")
    jdbcUpdate("GRANT SELECT ON tispark_test.* TO 'test_read'@'%'")
    jdbcUpdate("GRANT INSERT,CREATE ON tispark_test.* TO 'test_write'@'%'")

    jdbcUpdate("CREATE USER 'tispark_unit_test_user' IDENTIFIED BY ''")

    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
    jdbcUpdate(s"insert into $dbtable values(null, 'Hello'), (2, 'TiDB')")
  }

  override def afterAll(): Unit = {
    jdbcUpdate("DROP ROLE 'test_read', 'test_write'")
    jdbcUpdate(
      "DROP USER 'tispark_unit_test_user','tispark_unit_test_user'"
    )
    super.afterAll()
  }

  test("Select without privilege should not be passed") {
    an[SQLException] should be thrownBy {
      spark.sql(
        s"select * from `$databaseWithPrefix`.`$table`"
      )
    }
  }

  test("Use database and select without privilege should not be passed") {
    spark.sql(s"use $databaseWithPrefix")
    an[SQLException] should be thrownBy spark.sql(
      s"select * from $table"
    )
  }

  ignore("CreateTableLike without privilege should not be passed") {
    if (catalogPluginMode) {
      cancel
    }

    an[SQLException] should be thrownBy {
      spark.sql(
        s"create table `$databaseWithPrefix`.`${table}1`  like `$databaseWithPrefix`.`${table}`"
      )
    }

    jdbcUpdate(s"drop table if exsit `$database`.`${table}1`")
  }

  test("Give privilege") {
    jdbcUpdate(
      "GRANT 'test_read','test_write' TO 'tispark_unit_test_user'@'%';"
    )

    Thread.sleep((TiAuthorization.refreshInterval + 5) * 1000)
  }

  test("Select with privilege should be passed") {
    noException should be thrownBy spark.sql(
      s"select * from `$databaseWithPrefix`.`$table`"
    )
  }

  ignore("CreateTableLike with privilege should be passed") {
    if (catalogPluginMode) {
      cancel
    }

    noException should be thrownBy {
      spark.sql(
        s"create table `$databaseWithPrefix`.`${table}1`  like `$databaseWithPrefix`.`${table}`"
      )
    }

    jdbcUpdate(s"drop table if exsit `$database`.`${table}1`")
  }

}
