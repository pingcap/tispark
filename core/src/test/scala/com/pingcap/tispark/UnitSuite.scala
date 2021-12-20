package com.pingcap.tispark

import org.apache.spark.internal.Logging
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite, Matchers}
import org.slf4j.Logger

abstract class UnitSuite
    extends FunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Matchers
    with Logging {
  protected val logger: Logger = log
}
