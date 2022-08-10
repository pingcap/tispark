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

package com.pingcap.tispark.auth

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, Row}
import org.scalatest.Matchers.{
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
  val hive_table = "test_auth_hive"

  override def beforeAll(): Unit = {
    _isAuthEnabled = true
    _isHiveEnabled = true
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
  }

  override def afterAll(): Unit = {
    tidbStmt.execute(f"DROP USER IF EXISTS '$user'")
    tidbStmt.execute(s"DROP TABLE IF EXISTS `$database`.`$table`")
    tidbStmt.execute(s"DROP DATABASE IF EXISTS `$database`")
    tidbStmt.execute(s"DROP DATABASE IF EXISTS `$dummyDatabase`")
    super.afterAll()
    _isAuthEnabled = false
    _isHiveEnabled = false
    TiAuthorization.enableAuth = false
  }

  test("Operator on hive table should pass auth check") {
    spark.sql(s"CREATE TABLE IF NOT EXISTS `$hive_table`(i int, s varchar(255))")
    spark.sql(s"INSERT INTO `$hive_table` values(1,'1')")
    var count = spark.sql(s"select count(*) from `$hive_table`").head.get(0)
    assert(count == 1)
    the[Exception] thrownBy {
      spark.sql(s"delete from `$hive_table` where i=1")
    } should have message s"DELETE is only supported with v2 tables.;"
    spark.sql(s"DROP TABLE IF EXISTS `$hive_table`")
  }

  test("Use catalog should success") {
    spark.sql(s"use tidb_catalog.$dbPrefix$dummyDatabase")
    spark.sql(s"use tidb_catalog")
    spark.sql(s"use $dbPrefix$dummyDatabase")
  }

  test("Delete without DELETE & SELECT privilege should not be passed") {
    the[SQLException] thrownBy {
      spark.sql(s"delete from $dbtable where i = 3")
    } should have message s"SELECT command denied to user $user@% for table $dbtable"
  }

  test("Delete without SELECT privilege should not be passed") {
    tidbStmt.execute(f"GRANT DELETE on `$database`.`$table` TO '$user'@'%%';")
    Thread.sleep((TiAuthorization.refreshIntervalSecond + 2) * 1000)
    the[SQLException] thrownBy {
      spark.sql(s"delete from $dbtable where i = 3")
    } should have message s"SELECT command denied to user $user@% for table $dbtable"
    tidbStmt.execute(f"REVOKE DELETE ON `$database`.`$table` FROM '$user'@'%%';")
    Thread.sleep((TiAuthorization.refreshIntervalSecond + 2) * 1000)
  }

  test("Read Without SELECT privilege should not be passed") {
    the[SQLException] thrownBy {
      sqlContext.read
        .format("tidb")
        .option("database", database)
        .option("table", table)
        .options(tidbOptions)
        .load()
        .collect()
    } should have message s"SELECT command denied to user $user@% for table $dbtable"
  }

  test("Select without privilege should not be passed") {
    the[SQLException] thrownBy {
      spark.sql(s"select * from `$databaseWithPrefix`.`$table`")
    } should have message s"SELECT command denied to user $user@% for table $databaseWithPrefix.$table"
  }

  test("Get PD address from TiDB should be correct") {
    ti.tiAuthorization.get.getPDAddresses() should be(pdAddresses)
  }

  test("Use database and select without privilege should not be passed") {
    the[SQLException] thrownBy spark.sql(
      f"use $databaseWithPrefix") should have message s"Access denied for user $user@% to database ${databaseWithPrefix}"

    val caught = intercept[AnalysisException] {
      spark.sql(s"select * from $table")
    }
    // validateCatalog has been set namespace with "use tidb_catalog.$dbPrefix$dummyDatabase" in beforeAll() method
    assert(caught.getMessage.contains(s"Table or view not found: $table"))
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
    tidbStmt.execute(f"GRANT SELECT on `$database`.`$table` TO '$user'@'%%';")

    Thread.sleep((TiAuthorization.refreshIntervalSecond + 5) * 1000)
  }

  test("Read with SELECT privilege should be passed") {
    noException should be thrownBy {
      sqlContext.read
        .format("tidb")
        .option("database", database)
        .option("table", table)
        .options(tidbOptions)
        .load()
        .collect()
    }
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

    // spark 3.2 add isTemporary col when `show tables`, we need to exclude it.
    val tables = spark
      .sql(s"show tables")
      .drop("isTemporary")
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

  test("Delete without DELETE privilege should not be passed") {
    the[SQLException] thrownBy {
      spark.sql(s"delete from $dbtable where i = 3")
    } should have message s"DELETE command denied to user $user@% for table $dbtable"
  }

  test("Replace without DELETE & INSERT privilege should not be passed") {
    val schema = StructType(List(StructField("i", IntegerType), StructField("s", StringType)))
    val row = Row(4, "ReplaceWithoutPrivilege")
    val data: RDD[Row] = sc.makeRDD(List(row))
    val df = sqlContext.createDataFrame(data, schema)
    the[SQLException] thrownBy {
      df.write
        .format("tidb")
        .options(tidbOptions)
        .option("database", database)
        .option("table", table)
        .option("replace", "true")
        .mode("append")
        .save()
    } should have message s"INSERT command denied to user $user@% for table $dbtable"
  }

  test("Delete with DELETE & SELECT privilege should be passed") {
    tidbStmt.execute(f"GRANT DELETE on `$database`.`$table` TO '$user'@'%%';")
    Thread.sleep((TiAuthorization.refreshIntervalSecond + 2) * 1000)
    noException should be thrownBy spark.sql(s"delete from $dbtable where i = 3")
  }

  test("Insert without INSERT privilege should not be passed") {
    val schema = StructType(List(StructField("i", IntegerType), StructField("s", StringType)))
    val row = Row(4, "InsertWithoutPrivilege")
    val data: RDD[Row] = sc.makeRDD(List(row))
    val df = sqlContext.createDataFrame(data, schema)

    the[SQLException] thrownBy {
      df.write
        .format("tidb")
        .options(tidbOptions)
        .option("database", database)
        .option("table", table)
        .mode("append")
        .save()
    } should have message s"INSERT command denied to user $user@% for table $dbtable"
  }

  test("Insert with INSERT privilege should be passed") {
    tidbStmt.execute(f"GRANT INSERT on `$database`.`$table` TO '$user'@'%%';")
    Thread.sleep((TiAuthorization.refreshIntervalSecond + 2) * 1000)
    val schema = StructType(List(StructField("i", IntegerType), StructField("s", StringType)))
    val row = Row(4, "InsertWithPrivilege")
    val data: RDD[Row] = sc.makeRDD(List(row))
    val df = sqlContext.createDataFrame(data, schema)

    noException should be thrownBy {
      df.write
        .format("tidb")
        .options(tidbOptions)
        .option("database", database)
        .option("table", table)
        .mode("append")
        .save()
    }
  }

  test("Replace with DELETE & INSERT privilege should be passed") {
    Thread.sleep((TiAuthorization.refreshIntervalSecond + 2) * 1000)
    val schema = StructType(List(StructField("i", IntegerType), StructField("s", StringType)))
    val row = Row(4, "ReplaceWithPrivilege")
    val data: RDD[Row] = sc.makeRDD(List(row))
    val df = sqlContext.createDataFrame(data, schema)
    noException should be thrownBy {
      df.write
        .format("tidb")
        .options(tidbOptions)
        .option("database", database)
        .option("table", table)
        .option("replace", "true")
        .mode("append")
        .save()
    }
  }

}
