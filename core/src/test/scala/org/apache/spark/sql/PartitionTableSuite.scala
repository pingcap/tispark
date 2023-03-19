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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import com.pingcap.tikv.meta.TiDAGRequest
import org.apache.spark.sql.catalyst.plans.BasePlanTest

class PartitionTableSuite extends BasePlanTest {
  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists pt")
      tidbStmt.execute("drop table if exists pt2")
      tidbStmt.execute("drop table if exists p_t")
      tidbStmt.execute("drop table if exists pt3")
      tidbStmt.execute("drop table if exists pt4")
      tidbStmt.execute("drop table if exists t2")
      tidbStmt.execute("drop table if exists t3")
    } finally {
      super.afterAll()
    }

  test("reading from range column partition") {
    tidbStmt.execute("drop table if exists range_column_test")
    tidbStmt.execute(
      "create table range_column_test (id varchar(10)) partition by RANGE COLUMNS(`id`) (PARTITION `p1` VALUES LESS THAN ('''CN001'''),PARTITION `p2` VALUES LESS THAN ('CN002'))")
    tidbStmt.execute("insert into `range_column_test` values('CN001')")
    tidbStmt.execute("insert into `range_column_test` values('''CN001''')")

    judge("select * from range_column_test where id = 'CN001'")
    judge("select * from range_column_test where id = '\\'CN001\\''")
    judge("select * from range_column_test where id = 'CN002'")
  }

  test("reading from hash partition") {
    enablePartitionForTiDB()
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("create table t (id int) partition by hash(id) PARTITIONS 4")
    tidbStmt.execute("insert into `t` values(5)")
    tidbStmt.execute("insert into `t` values(15)")
    tidbStmt.execute("insert into `t` values(25)")
    tidbStmt.execute("insert into `t` values(35)")
    refreshConnections()

    judge("select * from t")
    judge("select * from t where id < 10")
  }

  test("read from partition table stack overflow") {
    val partSQL = (1 to 1023).map(i => s"PARTITION p$i VALUES LESS THAN ($i)").mkString(",")

    {
      // index scan
      tidbStmt.execute("DROP TABLE IF EXISTS `pt`")
      tidbStmt.execute(s"""
                          |CREATE TABLE `pt` (
                          |  `id` int(11) DEFAULT NULL,
                          |  `name` varchar(50) DEFAULT NULL,
                          |  `purchased` date DEFAULT NULL,
                          |  index `idx_id`(`id`)
                          |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                          |PARTITION BY RANGE (mod(year(purchased), 1023)) (
                          |$partSQL,
                          |PARTITION p1024 VALUES LESS THAN (MAXVALUE)
                          |)
                     """.stripMargin)

      tidbStmt.execute("insert into `pt` values(1, 'name', '1995-10-10')")
      refreshConnections()
      judge("select * from pt")
    }

    {
      // no index scan
      tidbStmt.execute("DROP TABLE IF EXISTS `pt`")
      tidbStmt.execute(s"""
                          |CREATE TABLE `pt` (
                          |  `id` int(11) DEFAULT NULL,
                          |  `name` varchar(50) DEFAULT NULL,
                          |  `purchased` date DEFAULT NULL
                          |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                          |PARTITION BY RANGE (mod(year(purchased), 1023)) (
                          |$partSQL,
                          |PARTITION p1024 VALUES LESS THAN (MAXVALUE)
                          |)
                     """.stripMargin)
      tidbStmt.execute("insert into `pt` values(1, 'name', '1995-10-10')")
      refreshConnections()
      judge("select * from pt")
    }
  }

  test("test read from range partition with function TO_DAYS()") {
    enablePartitionForTiDB()
    tidbStmt.execute("DROP TABLE IF EXISTS `pt`")
    tidbStmt.execute("""
        |CREATE TABLE `pt` (
        |  `id` int(11) DEFAULT NULL,
        |  `name` varchar(50) DEFAULT NULL,
        |  `purchased` date DEFAULT NULL,
        |  index `idx_id`(`id`)
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
        |PARTITION BY RANGE (TO_DAYS(purchased)) (
        |  PARTITION p0 VALUES LESS THAN (TO_DAYS('1990-01-01')),
        |  PARTITION p1 VALUES LESS THAN (TO_DAYS('1995-01-01')),
        |  PARTITION p2 VALUES LESS THAN (TO_DAYS('2000-01-01')),
        |  PARTITION p3 VALUES LESS THAN (MAXVALUE)
        |)
                     """.stripMargin)

    tidbStmt.execute("insert into `pt` values(1, 'name', '1995-10-10')")
    tidbStmt.execute("insert into `pt` values(1, 'name', '1985-10-10')")
    tidbStmt.execute("insert into `pt` values(1, 'name', '2005-10-10')")
    refreshConnections()

    judge("select * from pt")
    judge("select * from pt where purchased = date'1995-10-10'")
    judge("select * from pt where purchased != date'1995-10-10'")
    judge("select * from pt where purchased > date'1995-10-10'")
    judge("select * from pt where purchased <= date'1995-10-10'")
  }

  test(
    "test read from range partition and partition function (mod) is not supported by tispark") {
    enablePartitionForTiDB()
    tidbStmt.execute("DROP TABLE IF EXISTS `pt`")
    tidbStmt.execute("""
                       |CREATE TABLE `pt` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `purchased` date DEFAULT NULL,
                       |  index `idx_id`(`id`)
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE (mod(year(purchased), 4)) (
                       |  PARTITION p0 VALUES LESS THAN (1),
                       |  PARTITION p1 VALUES LESS THAN (2),
                       |  PARTITION p2 VALUES LESS THAN (3),
                       |  PARTITION p3 VALUES LESS THAN (MAXVALUE)
                       |)
                     """.stripMargin)

    tidbStmt.execute("insert into `pt` values(1, 'name', '1995-10-10')")
    refreshConnections()

    judge("select * from pt")
    judge("select * from pt where name = 'name'")
    judge("select * from pt where name != 'name'")
    judge("select * from pt where purchased = date'1995-10-10'")
    judge("select * from pt where purchased != date'1995-10-10'")
  }

  test("constant folding does not apply case") {
    enablePartitionForTiDB()
    tidbStmt.execute("drop table if exists t3")
    tidbStmt.execute(
      "create table t3 (c1 int) partition by range(c1) (partition p0 values less than maxvalue)")
    tidbStmt.execute("insert into `t3` values(2)")
    tidbStmt.execute("insert into `t3` values(3)")
    refreshConnections()

    judge("select * from t3 where c1 > 2 + c1")
  }

  test("single maxvalue partition table case and part expr is not column") {
    enablePartitionForTiDB()
    tidbStmt.execute(
      "create table t2 (c1 int) partition by range(c1 + 1) (partition p0 values less than maxvalue)")
    tidbStmt.execute("insert into `t2` values(2)")
    tidbStmt.execute("insert into `t2` values(3)")
    refreshConnections()

    judge("select * from t2 where c1 = 2")
  }

  // FIXME: https://github.com/pingcap/tispark/issues/701
  test("index scan on partition table") {
    enablePartitionForTiDB()
    tidbStmt.execute("drop table if exists p_t")
    tidbStmt.execute(
      "CREATE TABLE `p_t` (`id` int(11) DEFAULT NULL, `y` date DEFAULT NULL,   index `idx_y`(`y`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin PARTITION BY RANGE ( id ) (   PARTITION p0 VALUES LESS THAN (2),   PARTITION p1 VALUES LESS THAN (4),   PARTITION p2 VALUES LESS THAN (6) );")
    tidbStmt.execute("insert into `p_t` values(1, '1995-10-10')")
    tidbStmt.execute("insert into `p_t` values(2, '1996-10-10')")
    tidbStmt.execute("insert into `p_t` values(3, '1997-10-10')")
    tidbStmt.execute("insert into `p_t` values(4, '1998-10-10')")
    tidbStmt.execute("insert into `p_t` values(5, '1999-10-10')")
    refreshConnections()
    explainAndRunTest("select * from p_t where y = date'1996-10-10'", skipJDBC = true)
  }

  test("simple partition pruning test") {
    enablePartitionForTiDB()
    tidbStmt.execute(
      "CREATE TABLE `pt2` (   `id` int(11) DEFAULT NULL, `y` date DEFAULT NULL,   index `idx_y`(`y`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin " +
        "PARTITION BY RANGE ( id ) (   " +
        "PARTITION p0 VALUES LESS THAN (2),   " +
        "PARTITION p1 VALUES LESS THAN (4),   " +
        "PARTITION p2 VALUES LESS THAN (6) );")
    tidbStmt.execute("insert into `pt2` values(1, '1995-10-10')")
    tidbStmt.execute("insert into `pt2` values(2, '1996-10-10')")
    tidbStmt.execute("insert into `pt2` values(3, '1997-10-10')")
    tidbStmt.execute("insert into `pt2` values(4, '1998-10-10')")
    tidbStmt.execute("insert into `pt2` values(5, '1999-10-10')")
    refreshConnections()
    judge("select * from pt2 where y = date'1996-10-10' or id < 2 and id > 6")
  }

  private def extractDAGReq(df: DataFrame): TiDAGRequest = {
    enablePartitionForTiDB()
    extractDAGRequests(df).head
  }

  test("part pruning on date column") {
    enablePartitionForTiDB()
    tidbStmt.execute("DROP TABLE IF EXISTS `pt4`")
    try {
      tidbStmt.execute("""
                         |CREATE TABLE `pt4` (
                         |  `id` int(11) DEFAULT NULL,
                         |  `name` varchar(50) DEFAULT NULL,
                         |  `purchased` date DEFAULT NULL,
                         |  index `idx_pur`(`purchased`)
                         |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                         |PARTITION BY RANGE columns (purchased) (
                         |  PARTITION p0 VALUES LESS THAN ('1995-10-10'),
                         |  PARTITION p1 VALUES LESS THAN ('2000-10-10'),
                         |  PARTITION p2 VALUES LESS THAN ('2005-10-10'),
                         |  PARTITION p3 VALUES LESS THAN maxvalue
                         |)
                     """.stripMargin)
      refreshConnections()
      assert(extractDAGReq(spark
        .sql(
          "select * from pt4 where purchased < date'1994-10-10' or purchased > date'2994-10-10'")).getPrunedParts
        .size() == 2)

      assert(
        extractDAGReq(spark
          .sql("select * from pt4 where purchased < date'1994-10-10' and id < 10")).getPrunedParts
          .size() == 1)

      assert(
        extractDAGReq(spark
          .sql("select * from pt4 where purchased = date'1994-10-10'")).getPrunedParts
          .size() == 1)
    } catch {
      case _: java.sql.SQLException =>
      // ignore SQL exception for old version of TiDB
    }
  }

  test("part pruning on unix_timestamp") {
    enablePartitionForTiDB()
    tidbStmt.execute("DROP TABLE IF EXISTS `pt4`")
    tidbStmt.execute("""
                       |CREATE TABLE `pt4` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `purchased` timestamp DEFAULT NULL,
                       |  index `idx_id`(`id`)
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE (unix_timestamp(purchased)) (
                       |  PARTITION p0 VALUES LESS THAN (unix_timestamp('1995-10-10')),
                       |  PARTITION p1 VALUES LESS THAN (unix_timestamp('2000-10-10')),
                       |  PARTITION p2 VALUES LESS THAN (unix_timestamp('2005-10-10'))
                       |)
                     """.stripMargin)
    refreshConnections()

    assert(
      extractDAGReq(spark
        .sql("select * from pt4 where purchased = date'1994-10-10'")).getPrunedParts
        .size() == 3)
  }

  test("part pruning on year function and date type") {
    enablePartitionForTiDB()
    tidbStmt.execute("DROP TABLE IF EXISTS `pt3`")
    tidbStmt.execute("""
                       |CREATE TABLE `pt3` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `purchased` date DEFAULT NULL,
                       |  index `idx_id`(`id`)
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE (year(purchased)) (
                       |  PARTITION p0 VALUES LESS THAN (1995),
                       |  PARTITION p1 VALUES LESS THAN (2000),
                       |  PARTITION p2 VALUES LESS THAN (2005),
                       |  PARTITION p3 VALUES LESS THAN (MAXVALUE)
                       |)
                     """.stripMargin)
    refreshConnections()

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p.
          .sql("select * from pt3 where purchased = date'1994-10-10'")).getPrunedParts
        .get(0)
        .getName == "p0")

    assert(extractDAGReq(spark
    // expected part info only contains one part which is p1.
      .sql(
        "select * from pt3 where purchased > date'1996-10-10' and purchased < date'2000-10-10'")).getPrunedParts
      .get(0)
      .getName == "p1")

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains two parts which are p0 and p1.
          .sql("select * from pt3 where purchased < date'2000-10-10'")).getPrunedParts
      pDef.size() == 2 && pDef.get(0).getName == "p0" && pDef.get(1).getName == "p1"
    }

    assert {
      val pDef = extractDAGReq(spark
      // expected part info only contains one part which is p1.
        .sql(
          "select * from pt3 where purchased < date'2005-10-10' and purchased > date'2000-10-10'")).getPrunedParts
      pDef.size() == 1 && pDef.get(0).getName == "p2"
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // or with an unrelated column. All parts should be accessed.
          .sql("select * from pt3 where id < 4 or purchased < date'1995-10-10'")).getPrunedParts
      pDef.size() == 4
    }

    assert {
      val pDef = extractDAGReq(
        // for complicated expression, we do not support for now.
        // this will be improved later.
        spark
          .sql("select * from pt3 where year(purchased) < 1995")).getPrunedParts
      pDef.size() == 4
    }
  }

  test("part pruning on year function and datetime type") {
    enablePartitionForTiDB()
    tidbStmt.execute("DROP TABLE IF EXISTS `pt3`")
    tidbStmt.execute("""
                       |CREATE TABLE `pt3` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `purchased` datetime DEFAULT NULL,
                       |  index `idx_id`(`id`)
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE (year(purchased)) (
                       |  PARTITION p0 VALUES LESS THAN (1995),
                       |  PARTITION p1 VALUES LESS THAN (2000),
                       |  PARTITION p2 VALUES LESS THAN (2005),
                       |  PARTITION p3 VALUES LESS THAN (MAXVALUE)
                       |)
                     """.stripMargin)
    refreshConnections()

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p.
          .sql("select * from pt3 where purchased = date'1994-10-10'")).getPrunedParts
        .get(0)
        .getName == "p0")

    assert(extractDAGReq(spark
    // expected part info only contains one part which is p1.
      .sql(
        "select * from pt3 where purchased > date'1996-10-10' and purchased < date'2000-10-10'")).getPrunedParts
      .get(0)
      .getName == "p1")

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains two parts which are p0 and p1.
          .sql("select * from pt3 where purchased < date'2000-10-10'")).getPrunedParts
      pDef.size() == 2 && pDef.get(0).getName == "p0" && pDef.get(1).getName == "p1"
    }

    assert {
      val pDef = extractDAGReq(spark
      // expected part info only contains one part which is p1.
        .sql(
          "select * from pt3 where purchased < date'2005-10-10' and purchased > date'2000-10-10'")).getPrunedParts
      pDef.size() == 1 && pDef.get(0).getName == "p2"
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // or with an unrelated column. All parts should be accessed.
          .sql("select * from pt3 where id < 4 or purchased < date'1995-10-10'")).getPrunedParts
      pDef.size() == 4
    }

    assert {
      val pDef = extractDAGReq(
        // for complicated expression, we do not support for now.
        // this will be improved later.
        spark
          .sql("select * from pt3 where year(purchased) < 1995")).getPrunedParts
      pDef.size() == 4
    }
  }

  test("part pruning on to_days function and date type") {
    enablePartitionForTiDB()
    tidbStmt.execute("DROP TABLE IF EXISTS `pt_todays_date`")
    // to_days('2005-01-02') = 732313
    val s = ("""
               |CREATE TABLE `pt_todays_date` (
               |  `id` int(11) DEFAULT NULL,
               |  `name` varchar(50) DEFAULT NULL,
               |  `purchased` date DEFAULT NULL,
               |  index `idx_id`(`id`)
               |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
               |PARTITION BY RANGE (to_days(purchased)) (
               |  PARTITION p0 VALUES LESS THAN (to_days('1995-01-02')),
               |  PARTITION p1 VALUES LESS THAN (to_days('2000-01-02')),
               |  PARTITION p2 VALUES LESS THAN (732313),
               |  PARTITION p3 VALUES LESS THAN (MAXVALUE)
               |)
                     """.stripMargin)
    tidbStmt.execute("""
                       |CREATE TABLE `pt_todays_date` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `purchased` date DEFAULT NULL,
                       |  index `idx_id`(`id`)
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE (to_days(purchased)) (
                       |  PARTITION p0 VALUES LESS THAN (to_days('1995-01-02')),
                       |  PARTITION p1 VALUES LESS THAN (to_days('2000-01-02')),
                       |  PARTITION p2 VALUES LESS THAN (732313),
                       |  PARTITION p3 VALUES LESS THAN (MAXVALUE)
                       |)
                     """.stripMargin)

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains one part which is p0.
          .sql("select * from pt_todays_date where purchased = '1994-10-10'")).getPrunedParts
      pDef.size() == 1 && pDef.get(0).getName == "p0"
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains one part which is p1.
          .sql(
            "select * from pt_todays_date where purchased >= '2000-01-01 00:00:00' and purchased < '2000-01-02 23:59:59'")).getPrunedParts
      pDef.size() == 1 && pDef.get(0).getName == "p1"
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains one part which is p2.
          .sql("select * from pt_todays_date where purchased = '2005-01-01'")).getPrunedParts
      pDef.size() == 1 && pDef.get(0).getName == "p2"
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains one part which is p3.
          .sql("select * from pt_todays_date where purchased = '2005-01-02'")).getPrunedParts
      pDef.size() == 1 && pDef.get(0).getName == "p3"
    }

    // more than one part is pruned
    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains two parts which are p0 and p1.
          .sql("select * from pt_todays_date where purchased < '2000-01-02'")).getPrunedParts
      pDef.size() == 2 && pDef.get(0).getName == "p0" && pDef.get(1).getName == "p1"
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains two parts which are p0 p2 and p3.
          .sql(
            "select * from pt_todays_date where purchased = '1995-01-01' or purchased >= '2005-01-01' ")).getPrunedParts
      pDef.size() == 3 && pDef.get(0).getName == "p0" && pDef.get(1).getName == "p2" && pDef
        .get(2)
        .getName == "p3"
    }

    // not supported
    assert {
      val pDef = extractDAGReq(
        spark
        // or with an unrelated column. All parts should be accessed.
          .sql("select * from pt_todays_date where id < 4 or purchased < date'1995-10-10'")).getPrunedParts
      pDef.size() == 4
    }

  }

  test("part pruning on to_days function and datetime type") {
    enablePartitionForTiDB()
    tidbStmt.execute("DROP TABLE IF EXISTS `pt_todays_datetime`")
    tidbStmt.execute("""
        |CREATE TABLE `pt_todays_datetime` (
        |  `id` int(11) DEFAULT NULL,
        |  `name` varchar(50) DEFAULT NULL,
        |  `purchased` datetime DEFAULT NULL,
        |  index `idx_id`(`id`)
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
        |PARTITION BY RANGE (to_days(purchased)) (
        |  PARTITION p0 VALUES LESS THAN (to_days('1969-12-31')),
        |  PARTITION p1 VALUES LESS THAN (to_days('1970-01-01')),
        |  PARTITION p2 VALUES LESS THAN (to_days('1970-01-02')),
        |  PARTITION p3 VALUES LESS THAN (MAXVALUE)
        |)
                     """.stripMargin)

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains one part which is p0.
          .sql("select * from pt_todays_datetime where purchased = '1969-12-30 00:00:00'")).getPrunedParts
      pDef.size() == 1 && pDef.get(0).getName == "p0"
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains one part which is p1.
          .sql("select * from pt_todays_datetime where purchased = '1969-12-31 00:00:01'")).getPrunedParts
      pDef.size() == 1 && pDef.get(0).getName == "p1"
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains one part which is p2.
          .sql("select * from pt_todays_datetime where purchased = '1970-01-01 00:01:00'")).getPrunedParts
      pDef.size() == 1 && pDef.get(0).getName == "p2"
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains one part which is p3.
          .sql("select * from pt_todays_datetime where purchased = '1970-01-02 01:00:00'")).getPrunedParts
      pDef.size() == 1 && pDef.get(0).getName == "p3"
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains one part which is p2,p3
          .sql("select * from pt_todays_datetime where purchased >= '1970-01-01 01:00:00'")).getPrunedParts
      pDef.size() == 2 && pDef.get(0).getName == "p2" && pDef.get(1).getName == "p3"
    }
  }

  test("adding part pruning test when index is on partitioned column") {
    enablePartitionForTiDB()
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
      extractDAGReq(spark.sql("select * from p_t")).getPrunedParts
        .size() == 3)

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p.
          .sql("select * from p_t where id = 3")).getPrunedParts
        .get(0)
        .getName == "p1")

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p2.
          .sql("select * from p_t where id > 4")).getPrunedParts
        .get(0)
        .getName == "p2")

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains two parts which are p0 and p1.
          .sql("select * from p_t where id < 4")).getPrunedParts
      pDef.size() == 2 && pDef.get(0).getName == "p0" && pDef.get(1).getName == "p1"
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains one part which is p1.
          .sql("select * from p_t where id < 4 and id > 2")).getPrunedParts
      pDef.size() == 1 && pDef.get(0).getName == "p1"
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // or with an unrelated column. All parts should be accessed.
          .sql("select * from p_t where id < 4 and id > 2 or purchased = date'1995-10-10'")).getPrunedParts
      pDef.size() == 3
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // and with an unrelated column. only p1 should be accessed.
          .sql("select * from p_t where id < 4 and id > 2 and purchased = date'1995-10-10'")).getPrunedParts
      pDef.size() == 1 && pDef.get(0).getName == "p1"
    }
  }

  test("adding part pruning test") {
    enablePartitionForTiDB()
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

    assert {
      val pDef = extractDAGReq(
        spark
        // or with a unrelated column, all partition should be accessed.
          .sql("select * from p_t where id > 4 or id < 6 or purchased > date'1998-10-09'")).getPrunedParts
      pDef.size() == 3
    }

    assert {
      val pDef = extractDAGReq(
        spark
          .sql("select * from p_t where id > 4 and id < 6 and purchased > date'1998-10-09'")).getPrunedParts
      pDef.size() == 1
    }

    assert(
      extractDAGReq(spark.sql("select * from p_t")).getPrunedParts
        .size() == 3)

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p2.
          .sql("select * from p_t where id = 5")).getPrunedParts
        .get(0)
        .getName == "p2")

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p2.
          .sql("select * from p_t where id > 5")).getPrunedParts
        .get(0)
        .getName == "p2")

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains two parts which are p0 and p1.
          .sql("select * from p_t where id < 4")).getPrunedParts
      pDef.size() == 2 && pDef.get(0).getName == "p0" && pDef.get(1).getName == "p1"
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains one part which is p1.
          .sql("select * from p_t where id < 4 and id > 2")).getPrunedParts
      pDef.size() == 1 && pDef.get(0).getName == "p1"
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // expected parts info only contain two parts which is p0 and p2.
          .sql("select * from p_t where id < 2 or id > 4")).getPrunedParts
      pDef.size() == 2 && pDef.get(0).getName == "p0" && pDef.get(1).getName == "p2"
    }

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contain one part which is p1.
          .sql("select * from p_t where id > 2 and id < 4")).getPrunedParts
      pDef.size() == 1 && pDef.get(0).getName == "p1"
    }
  }

  test("partition read(w/o pruning)") {
    enablePartitionForTiDB()
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

  test("partition pruning with uppercase column") {
    tidbStmt.execute("DROP TABLE IF EXISTS `pt_uppercase_column`")
    tidbStmt.execute("""
                       |CREATE TABLE `pt_uppercase_column` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `ACT_DT` datetime DEFAULT NULL,
                       |  index `idx_id`(`id`)
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE (to_days(ACT_DT)) (
                       |  PARTITION p0 VALUES LESS THAN (to_days('1969-12-31')),
                       |  PARTITION p1 VALUES LESS THAN (to_days('1970-01-01')),
                       |  PARTITION p2 VALUES LESS THAN (to_days('1970-01-02')),
                       |  PARTITION p3 VALUES LESS THAN (MAXVALUE)
                       |)
                     """.stripMargin)

    assert {
      val pDef = extractDAGReq(
        spark
        // expected part info only contains one part which is p0.
          .sql("select * from pt_uppercase_column where ACT_DT = '1969-12-30 00:00:00'")).getPrunedParts
      pDef.size() == 1 && pDef.get(0).getName == "p0"
    }
  }

  def enablePartitionForTiDB(): Boolean =
    tidbStmt.execute("set @@tidb_enable_table_partition = 1")
}
