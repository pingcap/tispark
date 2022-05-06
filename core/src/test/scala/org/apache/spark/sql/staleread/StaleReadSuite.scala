package org.apache.spark.sql.staleread

import com.pingcap.tispark.write.TiDBOptions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, BaseTiSparkTest, Row}
import org.scalatest.Matchers.{be, convertToAnyShouldWrapper, have, include, noException, the}

import java.sql.Timestamp
import java.util.TimeZone

class StaleReadSuite extends BaseTiSparkTest {

  private val table = "stale_read_test"

  private val TIME_UNIT = 1000
  private val TIME_UNIT_HALF = TIME_UNIT / 2

  override def beforeAll(): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"))
    super.beforeAll()
  }

  override def beforeEach(): Unit = {
    createTable
    spark.conf.unset("spark.tispark.tidb_snapshot")
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT-7"))
    spark.conf.unset("spark.tispark.tidb_snapshot")
    super.afterAll()
  }

  test("stale read with timeStamp yyyy-mm-dd hh:mm:ss.sss") {
    val t = init()
    assert(2 == spark.sql(s"select count(*) from $table").collect().head.get(0))
    spark.conf.set("spark.tispark.tidb_snapshot", new Timestamp(t).toString)
    assert(1 == spark.sql(s"select count(*) from $table").collect().head.get(0))

  }

  test("stale read with invalid timeStamp yyyy-mm-dd hh:mm:ss.sss should throw exception") {
    spark.conf.set("spark.tispark.tidb_snapshot", "1999-13-25 25:00:00")
    assertThrows[IllegalArgumentException] { spark.sql(s"select count(*) from $table").collect() }
  }

  test("stale read with timeStamp long") {
    val t = init()
    assert(2 == spark.sql(s"select count(*) from $table").collect().head.get(0))
    spark.conf.set("spark.tispark.tidb_snapshot", t)
    assert(1 == spark.sql(s"select count(*) from $table").collect().head.get(0))
  }

  test("stale read supports statement level") {
    val (t1, t2) = init2()
    spark.conf.set("spark.tispark.tidb_snapshot", t2)
    assert(2 == spark.sql(s"select count(*) from $table").collect().head.get(0))
    spark.conf.set("spark.tispark.tidb_snapshot", t1)
    assert(1 == spark.sql(s"select count(*) from $table").collect().head.get(0))
    spark.conf.set("spark.tispark.tidb_snapshot", "")
    assert(3 == spark.sql(s"select count(*) from $table").collect().head.get(0))
    spark.conf.set("spark.tispark.tidb_snapshot", t2)
    assert(2 == spark.sql(s"select count(*) from $table").collect().head.get(0))
  }

  test("stale read supports session level") {
    val t = init()
    spark.conf.set("spark.tispark.tidb_snapshot", t)
    assert(1 == spark.sql(s"select count(*) from $table").collect().head.get(0))
    assert(1 == spark.sql(s"select count(*) from $table").collect().head.get(0))
    val spark2 = spark.newSession()
    spark2.sql("use tidb_catalog.tispark_test")
    assert(2 == spark2.sql(s"select count(*) from $table").collect().head.get(0))
    spark2.conf.set("spark.tispark.tidb_snapshot", t)
    assert(1 == spark2.sql(s"select count(*) from $table").collect().head.get(0))

  }

  test("stale read with schema change") {
    val (t0, t1, t2) = initDDL()

    spark.conf.set("spark.tispark.tidb_snapshot", t0)
    val caught = intercept[org.apache.spark.sql.AnalysisException] {
      spark.sql(s"select count(*) from $table").collect()
    }
    caught.getMessage() should include("Table or view not found")

    spark.conf.set("spark.tispark.tidb_snapshot", t1)
    assert(1 == spark.sql(s"select * from $table").schema.fields.length)
    assert(1 == spark.sql(s"select count(*) from $table").collect().head.get(0))

    spark.conf.set("spark.tispark.tidb_snapshot", t2)
    assert(2 == spark.sql(s"select * from $table").schema.fields.length)
    assert(2 == spark.sql(s"select count(*) from $table").collect().head.get(0))
  }

  ignore("stale read over gc life time should throw exception") {}

  test("stale read should not affect datasource api write") {
    val (t0, t1, t2) = initDDL()

    spark.conf.set("spark.tispark.tidb_snapshot", t1)
    noException should be thrownBy {
      tidbStmt.execute(s"insert into $table values (2,2)")
    }
    noException should be thrownBy {
      val schema = StructType(List(StructField("c", IntegerType), StructField("c1", IntegerType)))
      val data: RDD[Row] = sc.makeRDD(List(Row(3, 3)))
      val df = spark.createDataFrame(data, schema)
      df.write
        .format("tidb")
        .options(tidbOptions)
        .option("database", "tispark_test")
        .option("table", table)
        .mode("append")
        .save()
    }
    assert(1 == spark.sql(s"select count(*) from $table").collect().head.get(0))

    spark.conf.unset("spark.tispark.tidb_snapshot")
    assert(4 == spark.sql(s"select count(*) from $table").collect().head.get(0))

  }

  test("stale read should not affect datasource api read") {
    val t = init()
    spark.conf.set("spark.tispark.tidb_snapshot", t)
    assert(1 == spark.sql(s"select count(*) from $table").collect().head.get(0))
    val df = spark.read
      .format("tidb")
      .option(TiDBOptions.TIDB_DATABASE, "tispark_test")
      .option(TiDBOptions.TIDB_TABLE, table)
      .load()
    assert(2 == df.count())
  }

  private def createTable = {
    tidbStmt.execute(s"drop table if exists $table")
    tidbStmt.execute(s"create table $table (c int)")
  }

  private def init(): Long = {
    createTable
    tidbStmt.execute(s"insert into $table values (0)")
    Thread.sleep(TIME_UNIT)
    val t = System.currentTimeMillis() - TIME_UNIT_HALF
    tidbStmt.execute(s"insert into $table values (1)")
    t
  }

  private def init2(): (Long, Long) = {
    createTable
    tidbStmt.execute(s"insert into $table values (0)")
    Thread.sleep(TIME_UNIT)
    val t1 = System.currentTimeMillis() - TIME_UNIT_HALF

    tidbStmt.execute(s"insert into $table values (1)")
    Thread.sleep(TIME_UNIT)
    val t2 = System.currentTimeMillis() - TIME_UNIT_HALF

    tidbStmt.execute(s"insert into $table values (2)")
    (t1, t2)
  }

  private def initDDL(): (Long, Long, Long) = {
    tidbStmt.execute(s"drop table if exists $table")
    Thread.sleep(TIME_UNIT)
    val t0 = System.currentTimeMillis() - TIME_UNIT_HALF

    createTable
    tidbStmt.execute(s"insert into $table values (0)")
    Thread.sleep(TIME_UNIT)
    val t1 = System.currentTimeMillis() - TIME_UNIT_HALF

    tidbStmt.execute(s"alter table $table add column c1 int")
    tidbStmt.execute(s"insert into $table values (1, 1)")
    Thread.sleep(TIME_UNIT)
    val t2 = System.currentTimeMillis() - TIME_UNIT_HALF
    (t0, t1, t2)
  }
}
