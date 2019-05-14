package org.apache.spark.sql.jdbc

import com.pingcap.tispark.TiConfigConst
import org.apache.spark.sql.BaseTiSparkSuite
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}

class JDBCWriteSuite extends BaseTiSparkSuite {
  test("write into tidb using jdbc") {
    setCurrentDatabase("tispark_test")
    tidbStmt.execute("drop table if exists t1")
    tidbStmt.execute("drop table if exists t3")
    tidbStmt.execute("create table t1 (c1 int)")
    tidbStmt.execute("create table t3 (c1 int, d1 double)")
    tidbStmt.execute("insert into t1 values(2)")
    refreshConnections()

    spark.conf.set(TiConfigConst.TYPE_SYSTEM_VERSION, 1)
    val jdbcDf = spark.sql("select round(c1) from t3_j")
    val df1 = spark.sql("select * from t1")
    val rows = df1.collect()
    val schema = jdbcDf.schema
    val it = rows.toIterator
    var i = 0
    while (it.hasNext) {
      val value = it.next()
      schema.fields(i).dataType match {
        case LongType    => value.getLong(i)
        case IntegerType =>
        case DoubleType  => value.getDouble(i)
      }
      value.anyNull
      i += 1
    }

    val df2 = spark.sql("select * from t1")
    assert(df2.schema.fields(0).dataType == IntegerType)
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists t1")
      tidbStmt.execute("drop table if exists t2")
      tidbStmt.execute("drop table if exists t3")
    } finally {
      super.afterAll()
    }
}
