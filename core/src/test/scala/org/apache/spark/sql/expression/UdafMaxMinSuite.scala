package org.apache.spark.sql.expression

import org.apache.spark.sql.BaseInitialOnceTest

class UdafMaxMinSuite extends BaseInitialOnceTest {
  val testCases = loadQueryFromFile(
    "tispark-test",
    getClass.getSimpleName.substring(0, getClass.getSimpleName.indexOf("Suite")) + "Query"
  )
  private val allCases = testCases.split(";").filter(s => s.contains("select")).toSet

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }
}
