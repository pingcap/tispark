package org.apache.spark.sql

// NOTE: when you create new table, remember drop them at after all.
class PartPruningSuite extends BaseTiSparkSuite {
  test("adding part pruning test") {}

  test("part expr code gen test") {
    tidbStmt.execute("create table part_fn(d date)")
    tidbStmt.execute("insert into part_fn values ('1989-01-01'), ('2018-10-11')")
    refreshConnections()
    judge("select to_seconds(d) from part_fn")
    judge("select yearweek(d) from part_fn")
    judge("select weekday(d) from part_fn")
    judge("select to_days(d) from part_fn")
    judge("select microsecond(d) from part_fn")
    judge("select time_to_sec(d) from part_fn")
  }

  test("partition expr can be parsed by sparkSQLParser") {
    assert(spark.sql("select Abs(1)").count == 1)
    assert(spark.sql("select Ceiling(1)").count == 1)
    assert(spark.sql("SELECT datediff('2009-07-31', '2009-07-30')").count == 1)
    assert(spark.sql("SELECT day('2009-07-30')").count() == 1)
    assert(spark.sql("SELECT dayofmonth('2009-07-30')").count() == 1)
    assert(spark.sql("SELECT dayofweek('2009-07-30')").count() == 1)
    assert(spark.sql("SELECT dayofyear('2016-04-09')").count() == 1)
    assert(spark.sql("SELECT floor(-0.1)").count() == 1)
    assert(spark.sql("SELECT hour('2009-07-30 12:58:59')").count() == 1)
    assert(spark.sql("SELECT minute('2009-07-30 12:58:59')").count() == 1)
//     TODO 2 mod 1 is not supported.
    assert(spark.sql("SELECT mod(2, 1)").count() == 1)
    assert(spark.sql("SELECT month('2016-07-30')").count() == 1)
    assert(spark.sql("SELECT quarter('2016-08-31')").count() == 1)
    assert(spark.sql("SELECT second('2009-07-30 12:58:59')").count() == 1)
    assert(spark.sql("SELECT unix_timestamp('2016-04-08', 'yyyy-MM-dd')").count() == 1)
    assert(spark.sql("SELECT unix_timestamp()").count() == 1)
    assert(spark.sql("SELECT year('2009-07-30 12:58:59')").count() == 1)
//     extract is not supported
    // https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
    judge("select time_to_sec('12:30:49')")
    judge("select time_to_sec('2018-10-10 12:30:49')")
    judge("select to_days('2018-10-10')")
    judge("select to_days('2018-10-10 12:30:49')")
    judge("select to_seconds('2018-10-01 12:34:59')")
    judge("select to_seconds('2018-10-01')")
    judge("select microsecond('2018-10-01 12:34:59')")
    judge("select weekday('2018-09-12')")
    judge("select weekday('2018-01-01')")
    judge("select yearweek('1992-01-01')")
    judge("select yearweek('1992-12-31')")
    judge("select yearweek('1992-01-01', 1)")
    judge("select yearweek('1992-12-31', 1)")
    judge("select yearweek('1992-01-01', 2)")
    judge("select yearweek('1992-12-31', 2)")
    judge("select yearweek('1992-01-01', 3)")
    judge("select yearweek('1992-12-31', 3)")
    judge("select yearweek('1992-01-01', 4)")
    judge("select yearweek('1992-12-31', 4)")
    judge("select yearweek('1992-01-01', 5)")
    judge("select yearweek('1992-12-31', 5)")
    judge("select yearweek('1992-01-01', 6)")
    judge("select yearweek('1992-12-31', 6)")
    judge("select yearweek('1992-01-01', 7)")
    judge("select yearweek('1992-12-31', 7)")
  }

  test("partition read") {
    tidbStmt.execute("DROP TABLE IF EXISTS `partition_t`")
    tidbStmt.execute("""
                       |CREATE TABLE `partition_t` (
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
    tidbStmt.execute("insert into partition_t values (1, \"dede1\", \"1989-01-01\")")
    tidbStmt.execute("insert into partition_t values (2, \"dede2\", \"1991-01-01\")")
    tidbStmt.execute("insert into partition_t values (3, \"dede3\", \"1996-01-01\")")
    tidbStmt.execute("insert into partition_t values (4, \"dede4\", \"1998-01-01\")")
    tidbStmt.execute("insert into partition_t values (5, \"dede5\", \"2001-01-01\")")
    tidbStmt.execute("insert into partition_t values (6, \"dede6\", \"2006-01-01\")")
    tidbStmt.execute("insert into partition_t values (7, \"dede7\", \"2007-01-01\")")
    tidbStmt.execute("insert into partition_t values (8, \"dede8\", \"2008-01-01\")")
    refreshConnections()
    assert(spark.sql("select * from partition_t").count() == 8)
    judge("select count(*) from partition_t where id = 1", checkLimit = false)
    judge("select id from partition_t group by id", checkLimit = false)
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists `partition_t`")
      tidbStmt.execute("drop table if exists part_fn")
    } finally {
      super.afterAll()
    }
}
