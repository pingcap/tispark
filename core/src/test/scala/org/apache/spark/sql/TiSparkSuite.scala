package org.apache.spark.sql

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}

class TiSparkSuite extends SparkFunSuite
  with BeforeAndAfter with PrivateMethodTester with SharedSQLContext {

  override def beforeAll(): Unit = {
    super.beforeAll()

  }

  test("SELECT *") {
    sql("select tp_int from full_data_type_table")
  }
}
