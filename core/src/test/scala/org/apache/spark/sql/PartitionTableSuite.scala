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

package org.apache.spark.sql

import com.pingcap.tikv.meta.TiDAGRequest
import org.apache.spark.sql.execution.{CoprocessorRDD, RegionTaskExec}

class PartitionTableSuite extends BaseTiSparkSuite {
  test("index scan on partition table") {
    tidbStmt.execute(
      "CREATE TABLE `p_t` (   `id` int(11) DEFAULT NULL, `y` date DEFAULT NULL,   index `idx_y`(`y`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin PARTITION BY RANGE ( id ) (   PARTITION p0 VALUES LESS THAN (2),   PARTITION p1 VALUES LESS THAN (4),   PARTITION p2 VALUES LESS THAN (6) );"
    )
    tidbStmt.execute("insert into `p_t` values(1, '1995-10-10')")
    tidbStmt.execute("insert into `p_t` values(2, '1996-10-10')")
    tidbStmt.execute("insert into `p_t` values(3, '1997-10-10')")
    tidbStmt.execute("insert into `p_t` values(4, '1998-10-10')")
    tidbStmt.execute("insert into `p_t` values(5, '1999-10-10')")
    refreshConnections()
    judge("select * from p_t where y = date'1996-10-10'")
  }

  test("simple partition pruning test") {
    tidbStmt.execute(
      "CREATE TABLE `pt2` (   `id` int(11) DEFAULT NULL, `y` date DEFAULT NULL,   index `idx_y`(`y`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin " +
        "PARTITION BY RANGE ( id ) (   " +
        "PARTITION p0 VALUES LESS THAN (2),   " +
        "PARTITION p1 VALUES LESS THAN (4),   " +
        "PARTITION p2 VALUES LESS THAN (6) );"
    )
    tidbStmt.execute("insert into `pt2` values(1, '1995-10-10')")
    tidbStmt.execute("insert into `pt2` values(2, '1996-10-10')")
    tidbStmt.execute("insert into `pt2` values(3, '1997-10-10')")
    tidbStmt.execute("insert into `pt2` values(4, '1998-10-10')")
    tidbStmt.execute("insert into `pt2` values(5, '1999-10-10')")
    refreshConnections()
    judge("select * from pt2 where y = date'1996-10-10' or id < 2 and id > 6")
  }

  private def extractDAGReq(df: DataFrame): TiDAGRequest = {
    val executedPlan = df.queryExecution.executedPlan
    val copRDD = executedPlan.find(e => e.isInstanceOf[CoprocessorRDD])
    val regionTaskExec = executedPlan.find(e => e.isInstanceOf[RegionTaskExec])
    if (copRDD.isDefined) {
      copRDD.get
        .asInstanceOf[CoprocessorRDD]
        .tiRdd
        .dagRequest
    } else {
      regionTaskExec.get
        .asInstanceOf[RegionTaskExec]
        .dagRequest
    }
  }

  test("adding part pruning test when index is on partitioned column") {
    tidbStmt.execute("DROP TABLE IF EXISTS `p_t`")
    tidbStmt.execute("""
                       |CREATE TABLE `p_t` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `purchased` date DEFAULT NULL,
                       |  index `idx_id`(`id`)
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE (id) (
                       |  PARTITION p0 VALUES LESS THAN (2),
                       |  PARTITION p1 VALUES LESS THAN (4),
                       |  PARTITION p2 VALUES LESS THAN (6)
                       |)
                     """.stripMargin)
    refreshConnections()
    assert(
      extractDAGReq(
        spark.sql("select * from p_t")
      ).getPrunedParts
        .size() == 3
    )

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p.
          .sql("select * from p_t where id = 3")
      ).getPrunedParts
        .get(0)
        .getName == "p1"
    )

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p2.
          .sql("select * from p_t where id > 4")
      ).getPrunedParts
        .get(0)
        .getName == "p2"
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // expected part info only contains two parts which are p0 and p1.
            .sql("select * from p_t where id < 4")
        ).getPrunedParts
        pDef.size() == 2 && pDef.get(0).getName == "p0" && pDef.get(1).getName == "p1"
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // expected part info only contains one part which is p1.
            .sql(
              "select * from p_t where id < 4 and id > 2"
            )
        ).getPrunedParts
        pDef.size() == 1 && pDef.get(0).getName == "p1"
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // or with an unrelated column. All parts should be accessed.
            .sql(
              "select * from p_t where id < 4 and id > 2 or purchased = date'1995-10-10'"
            )
        ).getPrunedParts
        pDef.size() == 3
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // and with an unrelated column. only p1 should be accessed.
            .sql(
              "select * from p_t where id < 4 and id > 2 and purchased = date'1995-10-10'"
            )
        ).getPrunedParts
        pDef.size() == 1 && pDef.get(0).getName == "p1"
      }
    )
  }

  test("adding part pruning test") {
    tidbStmt.execute("DROP TABLE IF EXISTS `p_t`")
    tidbStmt.execute("""
                       |CREATE TABLE `p_t` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `purchased` date DEFAULT NULL
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE (id) (
                       |  PARTITION p0 VALUES LESS THAN (2),
                       |  PARTITION p1 VALUES LESS THAN (4),
                       |  PARTITION p2 VALUES LESS THAN (6)
                       |)
                     """.stripMargin)
    refreshConnections()

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // or with a unrelated column, all partition should be accessed.
            .sql(
              "select * from p_t where id > 4 or id < 6 or purchased > date'1998-10-09'"
            )
        ).getPrunedParts
        pDef.size() == 3
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
            .sql(
              "select * from p_t where id > 4 and id < 6 and purchased > date'1998-10-09'"
            )
        ).getPrunedParts
        pDef.size() == 1
      }
    )

    assert(
      extractDAGReq(
        spark.sql("select * from p_t")
      ).getPrunedParts
        .size() == 3
    )

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p2.
          .sql("select * from p_t where id = 5")
      ).getPrunedParts
        .get(0)
        .getName == "p2"
    )

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p2.
          .sql("select * from p_t where id > 5")
      ).getPrunedParts
        .get(0)
        .getName == "p2"
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // expected part info only contains two parts which are p0 and p1.
            .sql("select * from p_t where id < 4")
        ).getPrunedParts
        pDef.size() == 2 && pDef.get(0).getName == "p0" && pDef.get(1).getName == "p1"
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // expected part info only contains one part which is p1.
            .sql(
              "select * from p_t where id < 4 and id > 2"
            )
        ).getPrunedParts
        pDef.size() == 1 && pDef.get(0).getName == "p1"
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // expected parts info only contain two parts which is p0 and p2.
            .sql(
              "select * from p_t where id < 2 or id > 4"
            )
        ).getPrunedParts
        pDef.size() == 2 && pDef.get(0).getName == "p0" && pDef.get(1).getName == "p2"
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // expected part info only contain one part which is p1.
            .sql(
              "select * from p_t where id > 2 and id < 4"
            )
        ).getPrunedParts
        pDef.size() == 1 && pDef.get(0).getName == "p1"
      }
    )
  }

  //TODO: fn is not finished yet, comment them for now
//  test("part expr function code-gen test") {
//    tidbStmt.execute("create table part_fn(d date)")
//    tidbStmt.execute("insert into part_fn values ('1989-01-01'), ('2018-10-11')")
//    refreshConnections()
//    judge("select to_seconds(d) from part_fn")
//    judge("select yearweek(d) from part_fn")
//    judge("select weekday(d) from part_fn")
//    judge("select to_days(d) from part_fn")
//    judge("select microsecond(d) from part_fn")
//    judge("select time_to_sec(d) from part_fn")
//  }

//  test("partition expr can be parsed by sparkSQLParser") {
//    assert(spark.sql("select Abs(1)").count == 1)
//    assert(spark.sql("select Ceiling(1)").count == 1)
//    assert(spark.sql("SELECT datediff('2009-07-31', '2009-07-30')").count == 1)
//    assert(spark.sql("SELECT day('2009-07-30')").count() == 1)
//    assert(spark.sql("SELECT dayofmonth('2009-07-30')").count() == 1)
//    assert(spark.sql("SELECT dayofweek('2009-07-30')").count() == 1)
//    assert(spark.sql("SELECT dayofyear('2016-04-09')").count() == 1)
//    assert(spark.sql("SELECT floor(-0.1)").count() == 1)
//    assert(spark.sql("SELECT hour('2009-07-30 12:58:59')").count() == 1)
//    assert(spark.sql("SELECT minute('2009-07-30 12:58:59')").count() == 1)
//    assert(spark.sql("SELECT mod(2, 1)").count() == 1)
//    assert(spark.sql("SELECT month('2016-07-30')").count() == 1)
//    assert(spark.sql("SELECT quarter('2016-08-31')").count() == 1)
//    assert(spark.sql("SELECT second('2009-07-30 12:58:59')").count() == 1)
//    assert(spark.sql("SELECT unix_timestamp('2016-04-08', 'yyyy-MM-dd')").count() == 1)
//    assert(spark.sql("SELECT unix_timestamp()").count() == 1)
//    assert(spark.sql("SELECT year('2009-07-30 12:58:59')").count() == 1)
//    //     extract is not supported
//    // https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
//    judge("select time_to_sec('12:30:49')")
//    judge("select time_to_sec('2018-10-10 12:30:49')")
//    judge("select to_days('2018-10-10')")
//    judge("select to_days('2018-10-10 12:30:49')")
//    judge("select to_seconds('2018-10-01 12:34:59')")
//    judge("select to_seconds('2018-10-01')")
//    judge("select microsecond('2018-10-01 12:34:59')")
//    judge("select weekday('2018-09-12')")
//    judge("select weekday('2018-01-01')")
//    judge("select yearweek('1992-01-01')")
//    judge("select yearweek('1992-12-31')")
//    judge("select yearweek('1992-01-01', 1)")
//    judge("select yearweek('1992-12-31', 1)")
//    judge("select yearweek('1992-01-01', 2)")
//    judge("select yearweek('1992-12-31', 2)")
//    judge("select yearweek('1992-01-01', 3)")
//    judge("select yearweek('1992-12-31', 3)")
//    judge("select yearweek('1992-01-01', 4)")
//    judge("select yearweek('1992-12-31', 4)")
//    judge("select yearweek('1992-01-01', 5)")
//    judge("select yearweek('1992-12-31', 5)")
//    judge("select yearweek('1992-01-01', 6)")
//    judge("select yearweek('1992-12-31', 6)")
//    judge("select yearweek('1992-01-01', 7)")
//    judge("select yearweek('1992-12-31', 7)")
//  }

  test("partition read(w/o pruning)") {
    tidbStmt.execute("DROP TABLE IF EXISTS `p_t`")
    tidbStmt.execute("""
                       |CREATE TABLE `p_t` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `purchased` date DEFAULT NULL
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE ( `id` ) (
                       |  PARTITION p0 VALUES LESS THAN (1990),
                       |  PARTITION p1 VALUES LESS THAN (1995),
                       |  PARTITION p2 VALUES LESS THAN (2000),
                       |  PARTITION p3 VALUES LESS THAN (2005)
                       |)
                     """.stripMargin)
    tidbStmt.execute("insert into p_t values (1, \"dede1\", \"1989-01-01\")")
    tidbStmt.execute("insert into p_t values (2, \"dede2\", \"1991-01-01\")")
    tidbStmt.execute("insert into p_t values (3, \"dede3\", \"1996-01-01\")")
    tidbStmt.execute("insert into p_t values (4, \"dede4\", \"1998-01-01\")")
    tidbStmt.execute("insert into p_t values (5, \"dede5\", \"2001-01-01\")")
    tidbStmt.execute("insert into p_t values (6, \"dede6\", \"2006-01-01\")")
    tidbStmt.execute("insert into p_t values (7, \"dede7\", \"2007-01-01\")")
    tidbStmt.execute("insert into p_t values (8, \"dede8\", \"2008-01-01\")")
    refreshConnections()
    assert(spark.sql("select * from p_t").count() == 8)
    judge("select count(*) from p_t where id = 1", checkLimit = false)
    judge("select id from p_t group by id", checkLimit = false)
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists pt")
      tidbStmt.execute("drop table if exists pt2")
      tidbStmt.execute("drop table if exists p_t")
    } finally {
      super.afterAll()
    }
}
