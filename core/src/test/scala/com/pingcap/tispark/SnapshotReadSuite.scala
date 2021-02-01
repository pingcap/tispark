package com.pingcap.tispark

import com.pingcap.tikv.exception.{InvalidParameterException, TiBatchWriteException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{BaseTiSparkTest, Row}

import java.sql.Timestamp

class SnapshotReadSuite extends BaseTiSparkTest {
  private val TIME_UNIT = 2000L
  private val TIME_GC = 10L * 60L * 1000L

  test("test string type timestamp") {
    val (t1, t2, t3) = init3()

    // set time after t3
    spark.conf.set("spark.tispark.tidb_snapshot", newTimestampString(t3 + TIME_UNIT))
    assert(3 == spark.sql("select count(*) from t").collect().head.get(0))

    // set time after t2
    spark.conf.set("spark.tispark.tidb_snapshot", newTimestampString(t2 + TIME_UNIT))
    assert(2 == spark.sql("select count(*) from t").collect().head.get(0))

    // set time after t1
    spark.conf.set("spark.tispark.tidb_snapshot", newTimestampString(t1 + TIME_UNIT))
    assert(1 == spark.sql("select count(*) from t").collect().head.get(0))

    // set time empty string
    spark.conf.set("spark.tispark.tidb_snapshot", "")
    assert(3 == spark.sql("select count(*) from t").collect().head.get(0))
  }

  test("test long type timestamp") {
    val (t1, t2, t3) = init3()

    // set time after t3
    spark.conf.set("spark.tispark.tidb_snapshot", newTimestampLong(t3 + TIME_UNIT))
    assert(3 == spark.sql("select count(*) from t").collect().head.get(0))

    // set time after t2
    spark.conf.set("spark.tispark.tidb_snapshot", newTimestampLong(t2 + TIME_UNIT))
    assert(2 == spark.sql("select count(*) from t").collect().head.get(0))

    // set time after t1
    spark.conf.set("spark.tispark.tidb_snapshot", newTimestampLong(t1 + TIME_UNIT))
    assert(1 == spark.sql("select count(*) from t").collect().head.get(0))

    // set time empty string
    spark.conf.set("spark.tispark.tidb_snapshot", "")
    assert(3 == spark.sql("select count(*) from t").collect().head.get(0))
  }

  test("test invalid format of timestamp") {
    val t1 = init1()

    spark.conf.set("spark.tispark.tidb_snapshot", "invalid timestamp")
    intercept[InvalidParameterException] {
      spark.sql("select count(*) from t").collect()
    }
    spark.conf.set("spark.tispark.tidb_snapshot", "")
  }

  test("using spark sql api") {
    val (t1, t2, t3) = init3()

    // set time after t3
    spark.sql(s"SET spark.tispark.tidb_snapshot=${newTimestampString(t3 + TIME_UNIT)}")
    assert(3 == spark.sql("select count(*) from t").collect().head.get(0))

    // set time after t2
    spark.sql(s"SET spark.tispark.tidb_snapshot=${newTimestampLong(t2 + TIME_UNIT)}")
    assert(2 == spark.sql("select count(*) from t").collect().head.get(0))

    // set time after t1
    spark.sql(s"SET spark.tispark.tidb_snapshot=${newTimestampString(t1 + TIME_UNIT)}")
    assert(1 == spark.sql("select count(*) from t").collect().head.get(0))

    // set time empty string
    spark.sql("SET spark.tispark.tidb_snapshot=")
    assert(3 == spark.sql("select count(*) from t").collect().head.get(0))
  }

  test("multi SparkSession") {
    val (t1, t2, t3) = init3()

    val sparkSession1 = spark
    val sparkSession2 = sparkSession1.newSession()
    sparkSession2.sql(s"use ${dbPrefix}tispark_test")

    // sparkSession1: set time after t2
    sparkSession1.conf.set("spark.tispark.tidb_snapshot", newTimestampString(t2 + TIME_UNIT))
    assert(2 == sparkSession1.sql("select count(*) from t").collect().head.get(0))

    // sparkSession2: set time after t3
    sparkSession2.conf.set("spark.tispark.tidb_snapshot", newTimestampString(t3 + TIME_UNIT))
    assert(3 == sparkSession2.sql("select count(*) from t").collect().head.get(0))

    // sparkSession2: set time after t1
    sparkSession2.conf.set("spark.tispark.tidb_snapshot", newTimestampString(t1 + TIME_UNIT))
    assert(1 == sparkSession2.sql("select count(*) from t").collect().head.get(0))

    // sparkSession1: set time empty string
    sparkSession1.conf.set("spark.tispark.tidb_snapshot", "")
    assert(3 == sparkSession1.sql("select count(*) from t").collect().head.get(0))
  }

  test("ddl") {
    val (t1, t2, t3) = initDDL()

    // set time after t3
    {
      spark.conf.set("spark.tispark.tidb_snapshot", newTimestampString(t3 + TIME_UNIT))
      spark.sql("select * from t").show()
      val result = spark.sql("select * from t").collect()
      assert(3 == result.length)
      assert(2 == result.head.length)
      assert("c0".equals(result.head.schema(0).name))
      assert("c2".equals(result.head.schema(1).name))
    }

    // set time after t1
    {
      spark.conf.set("spark.tispark.tidb_snapshot", newTimestampString(t1 + TIME_UNIT))
      spark.sql("select * from t").show()
      val result = spark.sql("select * from t").collect()
      assert(1 == result.length)
      assert(2 == result.head.length)
      assert("c0".equals(result.head.schema(0).name))
      assert("c1".equals(result.head.schema(1).name))
    }

    // set time after t2
    {
      spark.conf.set("spark.tispark.tidb_snapshot", newTimestampString(t2 + TIME_UNIT))
      spark.sql("select * from t").show()
      val result = spark.sql("select * from t").collect()
      assert(2 == result.length)
      assert(1 == result.head.length)
      assert("c0".equals(result.head.schema(0).name))
    }

    // set time empty string
    {
      spark.conf.set("spark.tispark.tidb_snapshot", "")
      spark.sql("select * from t").show()
      val result = spark.sql("select * from t").collect()
      assert(3 == result.length)
      assert(2 == result.head.length)
      assert("c0".equals(result.head.schema(0).name))
      assert("c2".equals(result.head.schema(1).name))
    }
  }

  test("can not execute batch write when 'tidb_snapshot' is set") {
    val t1 = init1()

    // set time after t1
    spark.sql(s"SET spark.tispark.tidb_snapshot=${newTimestampString(t1 + TIME_UNIT)}")

    // call batch write
    val data: RDD[Row] = sc.makeRDD(List(Row(1L)))
    val schema = StructType(List(StructField("c", LongType)))
    val df = sqlContext.createDataFrame(data, schema)

    intercept[TiBatchWriteException] {
      df.write
        .format("tidb")
        .options(tidbOptions)
        .option("database", "tispark_test")
        .option("table", "t")
        .mode("append")
        .save()
    }
  }

  test("ddl + multi SparkSession") {
    val (t1, t2, t3) = initDDL()
    val sparkSession1 = spark
    val sparkSession2 = sparkSession1.newSession()
    sparkSession2.sql(s"use ${dbPrefix}tispark_test")

    // sparkSession1: set time after t2
    {
      sparkSession1.conf.set("spark.tispark.tidb_snapshot", newTimestampString(t2 + TIME_UNIT))
      sparkSession1.sql("select * from t").show()
      val result = sparkSession1.sql("select * from t").collect()
      assert(2 == result.length)
      assert(1 == result.head.length)
      assert("c0".equals(result.head.schema(0).name))
    }

    // sparkSession2: set time after t3
    {
      sparkSession2.conf.set("spark.tispark.tidb_snapshot", newTimestampString(t3 + TIME_UNIT))
      sparkSession2.sql("select * from t").show()
      val result = sparkSession2.sql("select * from t").collect()
      assert(3 == result.length)
      assert(2 == result.head.length)
      assert("c0".equals(result.head.schema(0).name))
      assert("c2".equals(result.head.schema(1).name))
    }

    // sparkSession2: set time after t1
    {
      sparkSession2.conf.set("spark.tispark.tidb_snapshot", newTimestampString(t1 + TIME_UNIT))
      sparkSession2.sql("select * from t").show()
      val result = sparkSession2.sql("select * from t").collect()
      assert(1 == result.length)
      assert(2 == result.head.length)
      assert("c0".equals(result.head.schema(0).name))
      assert("c1".equals(result.head.schema(1).name))
    }

    // sparkSession2: set time after t2
    {
      sparkSession2.conf.set("spark.tispark.tidb_snapshot", newTimestampString(t2 + TIME_UNIT))
      sparkSession2.sql("select * from t").show()
      val result = sparkSession2.sql("select * from t").collect()
      assert(2 == result.length)
      assert(1 == result.head.length)
      assert("c0".equals(result.head.schema(0).name))
    }

    // sparkSession1: set time empty string
    {
      sparkSession1.conf.set("spark.tispark.tidb_snapshot", "")
      sparkSession1.sql("select * from t").show()
      val result = sparkSession1.sql("select * from t").collect()
      assert(3 == result.length)
      assert(2 == result.head.length)
      assert("c0".equals(result.head.schema(0).name))
      assert("c2".equals(result.head.schema(1).name))
    }
  }

  test("read snapshot before create table") {
    val tableName = s"t_${System.currentTimeMillis()}"
    tidbStmt.execute(s"drop table if exists $tableName")
    tidbStmt.execute(s"create table $tableName (c int)")
    tidbStmt.execute(s"insert into $tableName values (1)")
    val t1 = System.currentTimeMillis()

    spark.conf.set("spark.tispark.tidb_snapshot", newTimestampString(t1 - TIME_GC / 2))
    val caught = intercept[Throwable] {
      spark.sql(s"select * from $tableName").collect()
    }
    assert(
      caught.getMessage
        .equals(s"Table or view '$tableName' not found in database 'tidb_tispark_test';"))

    tidbStmt.execute(s"drop table if exists $tableName")
  }

  ignore("snapshot is older than GC safe point") {
    val t1 = init1()

    // snapshot is older than GC safe point
    spark.conf.set("spark.tispark.tidb_snapshot", newTimestampString(t1 - TIME_GC / 2))
    // TODO: mars throw error
    spark.sql("select count(*) from t").show(200, truncate = false)
  }

  ignore("执行过程中需要阻止 GC 清除该 Snapshot") {
    // TODO: mars
  }

  ignore("timezone problem") {
    // TODO: mars
  }

  ignore("distributed cluser test") {
    // TODO: mars
  }

  private def init1(): Long = {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("create table t (c int)")
    tidbStmt.execute("insert into t values (1)")
    val t1 = System.currentTimeMillis()
    t1
  }

  private def init3(): (Long, Long, Long) = {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("create table t (c int)")
    tidbStmt.execute("insert into t values (1)")
    val t1 = System.currentTimeMillis()
    Thread.sleep(2 * TIME_UNIT)
    tidbStmt.execute("insert into t values (2)")
    val t2 = System.currentTimeMillis()
    Thread.sleep(2 * TIME_UNIT)
    tidbStmt.execute("insert into t values (3)")
    val t3 = System.currentTimeMillis()
    (t1, t2, t3)
  }

  private def initDDL(): (Long, Long, Long) = {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("create table t (c0 int, c1 int)")
    tidbStmt.execute("insert into t values (1, 11)")
    val t1 = System.currentTimeMillis()
    Thread.sleep(2 * TIME_UNIT)
    tidbStmt.execute("alter table t drop column c1")
    tidbStmt.execute("insert into t values (2)")
    val t2 = System.currentTimeMillis()
    Thread.sleep(2 * TIME_UNIT)
    tidbStmt.execute("alter table t add column c2 varchar(64)")
    tidbStmt.execute("insert into t values (3, '333')")
    val t3 = System.currentTimeMillis()
    (t1, t2, t3)
  }

  private def newTimestampString(ts: Long): String = {
    val t = new Timestamp(ts).toString
    logInfo(s"set spark.tispark.tidb_snapshot = $t")
    t
  }

  private def newTimestampLong(ts: Long): String = {
    val t = ts.toString
    logInfo(s"set spark.tispark.tidb_snapshot = $t")
    t
  }

  override def afterAll(): Unit = {
    try {
      spark.conf.set("spark.tispark.tidb_snapshot", "")
      tidbStmt.execute("drop table if exists t")
    } finally {
      super.afterAll()
    }
  }
}
