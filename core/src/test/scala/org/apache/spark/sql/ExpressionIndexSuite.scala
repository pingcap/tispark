/*
 * Copyright 2020 PingCAP, Inc.
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

class ExpressionIndexSuite extends BaseTiSparkTest {

  test("expression index") {
    if (!supportExpressionIndex) {
      cancel("current version of TiDB does not support expression index!")
    }

    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("create table t (name varchar(64));")
    tidbStmt.execute("CREATE INDEX idx ON t ((lower(name)));")
    tidbStmt.execute("insert into t values ('a'), ('b'), ('PingCAP');")

    val query = "select * from t  where lower(name) = 'pingcap';"
    spark.sql(s"explain $query").show(false)
    spark.sql(query).show()
    val queryResult = spark.sql(query).collect()
    assert("PingCAP".equals(queryResult.head.getString(0)))
    assert(1 == queryResult.head.size)

    val tiTableInfo = ti.meta.getTable(dbPrefix + "tispark_test", "t").get
    assert(tiTableInfo.getColumns.size() == 1)
    assert(tiTableInfo.getColumns(true).size() == 2)
    assert(tiTableInfo.getIndices.size() == 0)
    assert(tiTableInfo.getIndices(true).size() == 1)
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists t")
    } finally {
      super.afterAll()
    }
}
