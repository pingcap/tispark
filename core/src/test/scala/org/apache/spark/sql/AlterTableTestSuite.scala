/*
 * Copyright 2018 PingCAP, Inc.
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

package org.apache.spark.sql

class AlterTableTestSuite extends BaseTiSparkTest {

  def alterTable(
      dataType: String,
      value: String,
      defaultVal: String,
      defaultVal2: String,
      nullable: Boolean = true,
      defaultNullOnly: Boolean = false): Unit = {
    tidbStmt.execute(s"drop table if exists t")
    tidbStmt.execute(s"create table t(c1 $dataType default $defaultVal)")
    tidbStmt.execute(s"insert into t values()")
    tidbStmt.execute(s"insert into t values($value)")
    if (nullable) {
      tidbStmt.execute(s"insert into t values(null)")
      refreshConnections() // refresh since we need to load data again
      judge("select * from t")
      tidbStmt.execute(s"alter table t add column c2 $dataType default null")
      refreshConnections()
      judge("select * from t")
      tidbStmt.execute(s"alter table t drop column c2")
    }
    if (!defaultNullOnly) {
      refreshConnections() // refresh since we need to load data again
      judge("select * from t")
      tidbStmt.execute(s"alter table t add column c2 $dataType default $defaultVal2")
      refreshConnections()
      judge("select * from t")
    }
  }

  // https://github.com/pingcap/tispark/issues/313
  // https://github.com/pingcap/tikv-client-lib-java/issues/198
  test("Default value information not fetched") {
    alterTable("varchar(45)", "\"a\"", "\"b\"", "\"c\"")
    alterTable(
      "datetime",
      "\"2018-01-01 00:00:00\"",
      "\"2018-01-01 11:11:11\"",
      "\"2018-01-01 22:22:22\"")
    alterTable("date", "\"2018-01-01\"", "\"2018-02-02\"", "\"2018-03-03\"")
    alterTable(
      "timestamp",
      "\"2018-01-01 00:00:00\"",
      "\"2018-01-01 11:11:11\"",
      "\"2018-01-01 22:22:22\"",
      nullable = false)
    alterTable("bigint", "9223372036854775807", "-9223372036854775808", "1")
    alterTable("decimal(20,3)", "12345678.123", "2345678.321", "-12345678.3")
    alterTable("double", "0.1", "1.2", "3.4")
    alterTable("float", "0.1", "1.2", "3.4")
    alterTable("int", "0", "1", "3")
    alterTable("mediumint", "0", "1", "3")
    alterTable("real", "0.1", "1.2", "3.4")
    alterTable("smallint", "0", "1", "3")
    alterTable("tinyint", "b\'0\'", "b\'1\'", "b\'0\'")
    alterTable("char(10)", "\"a\"", "\"b\"", "\"c\"")
    alterTable("varchar(40)", "\"a\"", "\"b\"", "\"c\"")
    alterTable("text", "\"0\"", "null", "", defaultNullOnly = true)
    alterTable("blob", "\"0\"", "null", "", defaultNullOnly = true)
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists t")
    } finally {
      super.afterAll()
    }
}
