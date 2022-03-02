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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.Matchers.{
  an,
  be,
  contain,
  convertToAnyShouldWrapper,
  have,
  noException,
  not,
  the
}

import java.sql.SQLException

class TiAuthIntegrationSuite extends SharedSQLContext {
  val table = "test_auth_basic"
  val database = "tispark_test_auth"
  val invisibleTable = "test_auth_basic_invisible"
  val dbtable = f"$database.$table"
  val databaseWithPrefix = f"$dbPrefix$database"
  val dummyDatabase = "tispark_test_auth_dummy"
  val user = "tispark_unit_test_user"

  override def beforeAll(): Unit = {
    _isAuthEnabled = true
    super.beforeAll()

    // set sql conf
    spark.sqlContext.setConf("spark.sql.tidb.addr", "127.0.0.1")
    spark.sqlContext.setConf("spark.sql.tidb.port", "4000")
    spark.sqlContext.setConf("spark.sql.tidb.user", user)
    spark.sqlContext.setConf("spark.sql.tidb.password", "")

    // create database
    tidbStmt.execute(s"CREATE DATABASE IF NOT EXISTS `$database`")
    tidbStmt.execute(s"CREATE DATABASE IF NOT EXISTS `$dummyDatabase`")

    // create table
    tidbStmt.execute(
      s"create table IF NOT EXISTS $database.$invisibleTable(i int, s varchar(128))")
    tidbStmt.execute(s"create table IF NOT EXISTS $dbtable(i int, s varchar(128))")
    tidbStmt.execute(s"insert into $dbtable values(null, 'Hello'), (2, 'TiDB')")

    // create user
    tidbStmt.execute(f"CREATE USER IF NOT EXISTS '$user' IDENTIFIED BY ''")

    // grant user
    tidbStmt.execute(f"GRANT CREATE ON $dummyDatabase.* TO '$user'@'%%'")
    tidbStmt.execute(f"GRANT PROCESS ON *.* TO '$user'@'%%'")

    spark.sql(s"use tidb_catalog.$dbPrefix$dummyDatabase")

  }

  override def afterAll(): Unit = {
    tidbStmt.execute(f"DROP USER IF EXISTS '$user'")
    tidbStmt.execute(s"DROP TABLE IF EXISTS `$database`.`$table`")
    tidbStmt.execute(s"DROP DATABASE IF EXISTS `$database`")
    tidbStmt.execute(s"DROP DATABASE IF EXISTS `$dummyDatabase`")
    super.afterAll()
    _isAuthEnabled = false
    TiAuthorization.enableAuth = false
  }

  test("Use catalog should success") {
    spark.sql(s"use tidb_catalog")
    spark.sql(s"use $dbPrefix$dummyDatabase")
  }

  test("Select without privilege should not be passed") {
    the[SQLException] thrownBy {
      spark.sql(s"select * from `$databaseWithPrefix`.`$table`")
    } should have message s"SELECT command denied to user $user@% for table $databaseWithPrefix.$table"
  }

  test("Get PD address from TiDB should be correct") {
    ti.tiAuthorization.get.getPDAddress() should be(pdAddresses)
  }

  test("Use database and select without privilege should not be passed") {
    the[SQLException] thrownBy spark.sql(
      f"use $databaseWithPrefix") should have message s"Access denied for user $user@% to database ${databaseWithPrefix}"
    val caught = intercept[AnalysisException] {
      spark.sql(s"select * from $table")
    }
    // validateCatalog has been set namespace with "use tidb_catalog.$dbPrefix$dummyDatabase" in beforeAll() method
    if (validateCatalog) {
      assert(caught.getMessage.contains(s"Table or view not found: test_auth_basic"))
    }
  }

  test(f"Show databases without privilege should not contains db") {
    val databases = spark
      .sql(s"show databases")
      .collect()
      .map(row => row.toString())
      .toList
    databases should not contain (f"[$databaseWithPrefix]")
  }

  test("Give privilege") {
    tidbStmt.execute(f"GRANT UPDATE,SELECT on `$database`.`$table` TO '$user'@'%%';")

    Thread.sleep((TiAuthorization.refreshIntervalSecond + 5) * 1000)
  }

  test("Select with privilege should be passed") {
    noException should be thrownBy spark.sql(s"select * from `$databaseWithPrefix`.`$table`")
  }

  test("Select case insensitive with privilege should be passed") {
    noException should be thrownBy spark.sql(
      s"select * from `$databaseWithPrefix`.`${table.toUpperCase()}`")
  }

  test("Use database and select with privilege should be passed") {
    noException should be thrownBy spark.sql(s"use $databaseWithPrefix")
    noException should be thrownBy spark.sql(s"select * from $table")
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
    if (validateCatalog) {
      tables should contain(f"[$databaseWithPrefix,$table]")
      tables should not contain (f"[$databaseWithPrefix,$invisibleTable]")
    }
  }

  test(f"Describe tables should not success with invisible table") {
    noException should be thrownBy spark.sql(s"DESCRIBE TABLE `$databaseWithPrefix`.`$table`")
    the[SQLException] thrownBy spark.sql(
      s"DESCRIBE `$databaseWithPrefix`.`$invisibleTable`") should have message s"SELECT command denied to user $user@% for table $databaseWithPrefix.$invisibleTable"
  }
}
