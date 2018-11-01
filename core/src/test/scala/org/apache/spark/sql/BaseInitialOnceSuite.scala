package org.apache.spark.sql

class BaseInitialOnceSuite extends BaseTiSparkSuite {
  private var init = false

  override def beforeAll(): Unit =
    if (!init) {
      super.beforeAll()
      init = true
    }
}
