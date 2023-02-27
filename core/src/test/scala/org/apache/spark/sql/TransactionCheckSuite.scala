package org.apache.spark.sql

import com.pingcap.tispark.TiConfigConst
import org.scalatest.Matchers.{message, the}

class TransactionCheckSuite extends BaseTiSparkTest {
  private val table = "transaction_check_test"

  val intervalExceed: Int = 40 * 60 * 1000
  // gc_life_time -1 = 9min
  val intervalPass: Int = 9 * 60 * 1000

  override def beforeEach(): Unit = {
    super.beforeEach()
    tidbStmt.execute(s"drop table if exists $table")
    tidbStmt.execute(s"create table $table (c int)")
  }

  // use stale read to test if TiSpark will throw exception when start_ts < gc_safe_point
  test("transaction check throw exception when start_ts < gc_safe_point") {
    spark.conf.set(TiConfigConst.STALE_READ, System.currentTimeMillis() - intervalExceed)
    try {
      val exception = the[Exception] thrownBy {
        spark.sql(s"select * from $table").show()
      }
      assert(exception.getCause.getMessage.contains("start_ts < gc_safe_point"))
    } finally {
      spark.conf.unset(TiConfigConst.STALE_READ)
    }
  }

  test("pass transaction check when start_ts >= gc_safe_point") {
    spark.conf.set(TiConfigConst.STALE_READ, System.currentTimeMillis() - intervalPass)
    try {
      spark.sql(s"select * from $table").show()
    } finally {
      spark.conf.unset(TiConfigConst.STALE_READ)
    }
  }

}
