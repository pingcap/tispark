package org.apache.spark.sql

class BaseInitialOnceTest extends BaseTiSparkTest {
  private var init = false

  override def beforeAll(): Unit =
    if (!init) {
      super.beforeAll()
      init = true
    }
}
