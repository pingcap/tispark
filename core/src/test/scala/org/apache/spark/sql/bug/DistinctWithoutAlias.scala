/*
 * Copyright 2019 PingCAP, Inc.
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

package org.apache.spark.sql.bug

import org.apache.spark.sql.BaseTiSparkTest

class DistinctWithoutAlias extends BaseTiSparkTest {

  test("Distinct without alias throws NullPointerException") {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("create table t(c1 bigint);")
    tidbStmt.execute("insert into t values (2), (3), (2);")

    val sqls = //"select distinct(c1) as d, 1 as w from t" ::
      //"select c1 as d, 1 as w from t group by c1" ::
      //"select c1, 1 as w from t group by c1" ::
      "select distinct(c1), 1L as w from t" ::
        Nil

    for (sql <- sqls) {
      explainTestAndCollect(sql, extended = true)
      //compSparkWithTiDB(sql)
    }
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists t")
    } finally {
      super.afterAll()
    }
}
