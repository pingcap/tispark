package com.pingcap.tispark.auth

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.Matchers.{an, be, contain, convertToAnyShouldWrapper, noException, not}

import java.sql.SQLException

class TiAuthIntegrationSuite extends SharedSQLContext {
  val table = "test_auth_basic"
  val database = "tispark_test_auth"
  val invisibleTable = "test_auth_basic_invisible"
  val dbtable = f"$database.$table"
  val databaseWithPrefix = f"$dbPrefix$database"
  val dummyDatabase = "tispark_test_auth_dummy"

  override def beforeAll(): Unit = {
    _isAuthEnabled = true
    super.beforeAll()
    // set tidb config
    spark.sqlContext.setConf("tidb.addr", "127.0.0.1")
    spark.sqlContext.setConf("tidb.port", "4000")
    spark.sqlContext.setConf("tidb.user", "tispark_unit_test_user")
    spark.sqlContext.setConf("tidb.password", "")

    // create database
    tidbStmt.execute(s"CREATE DATABASE IF NOT EXISTS `$database`")
    tidbStmt.execute(s"CREATE DATABASE IF NOT EXISTS `$dummyDatabase`")

    // create table
    tidbStmt.execute(s"create table $database.$invisibleTable(i int, s varchar(128))")
    tidbStmt.execute(s"create table $dbtable(i int, s varchar(128))")
    tidbStmt.execute(s"insert into $dbtable values(null, 'Hello'), (2, 'TiDB')")

    // create role
    tidbStmt.execute(f"CREATE ROLE 'test_read', 'test_write'")
    tidbStmt.execute(f"GRANT SELECT ON $database.$table TO 'test_read'@'%%'")
    tidbStmt.execute(
      f"GRANT UPDATE ON $database.$table TO 'test_write'@'%%'"
    )

    // create user
    tidbStmt.execute("CREATE USER 'tispark_unit_test_user' IDENTIFIED BY ''")

    // grant user
    tidbStmt.execute(
      f"GRANT CREATE ON $dummyDatabase.* TO 'tispark_unit_test_user'@'%%'"
    )

    // set namespace "tidb_catalog"
    if (catalogPluginMode) {
      spark.sql(s"use tidb_catalog.$dbPrefix$dummyDatabase")
    }
  }

  override def afterAll(): Unit = {
    tidbStmt.execute("DROP ROLE 'test_read', 'test_write'")
    tidbStmt.execute(
      "DROP USER 'tispark_unit_test_user'"
    )
    tidbStmt.execute(s"DROP TABLE IF EXISTS `$database`.`$table`")
    tidbStmt.execute(s"DROP DATABASE IF EXISTS `$database`")
    tidbStmt.execute(s"DROP DATABASE IF EXISTS `$dummyDatabase`")
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
    an[SQLException] should be thrownBy spark.sql(s"use $databaseWithPrefix")
    an[AnalysisException] should be thrownBy spark.sql(
      s"select * from $table"
    )
  }

  test(f"Show databases without privilege should not contains db") {
    val databases = spark
      .sql(s"show databases")
      .collect()
      .map(row => row.toString())
      .toList
    databases should not contain (f"[$databaseWithPrefix]")
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

    tidbStmt.execute(s"drop table if exsit `$database`.`${table}1`")
  }

  test("Give privilege") {
    tidbStmt.execute(
      "GRANT 'test_read','test_write' TO 'tispark_unit_test_user'@'%';"
    )

    Thread.sleep((TiAuthorization.refreshInterval + 5) * 1000)
  }

  test("Select with privilege should be passed") {
    noException should be thrownBy spark.sql(
      s"select * from `$databaseWithPrefix`.`$table`"
    )
  }

  test("Use database and select with privilege should not be passed") {
    noException should be thrownBy spark.sql(s"use $databaseWithPrefix")
    noException should be thrownBy spark.sql(
      s"select * from $table"
    )
  }

  test(f"Show databases with privilege should contains db") {
    val databases = spark
      .sql(s"show databases")
      .collect()
      .map(row => row.toString())
      .toList
    databases should contain(f"[$databaseWithPrefix]")
  }

  test(f"Show tables should not contain invisible table") {
    noException should be thrownBy spark.sql(s"use $databaseWithPrefix")

    val tables = spark
      .sql(s"show tables")
      .collect()
      .map(row => row.toString())
      .toList
    tables should contain (f"[$table]")
    tables should not contain (f"[$invisibleTable]")
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

    tidbStmt.execute(s"drop table if exsit `$database`.`${table}1`")
  }
}
