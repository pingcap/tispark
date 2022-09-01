package com.pingcap.tispark.accumulator;

import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{AppenderSkeleton, Logger}
import org.apache.spark.sql.BaseTiSparkTest
import org.apache.spark.sql.test.SharedSQLContext

import java.util
import java.util.stream.Collectors;

class AccumulatorSuite extends BaseTiSparkTest{
  test("cacheInvalidateCallback does not work") {
    val listLogAppender = new ListLogAppender
    val logger = Logger.getRootLogger
    logger.addAppender(listLogAppender)
    try {
      tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
      tidbStmt.execute(
        """
          |CREATE TABLE `t1` (
          |`a` BIGINT(20)  NOT NULL,
          |`b` varchar(255) NOT NULL,
          |`c` varchar(255) DEFAULT NULL
          |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
              """.stripMargin)
      spark.sql("SELECT * FROM t1 where a>0 and b > 'aa'").show()
      tidbStmt.execute(
        "SPLIT TABLE t1 BETWEEN (-9223372036854775808) AND (-8223372036854775808) REGIONS 300")
      spark.sql("SELECT * FROM t1 where a>0 and b > 'aa'").show()
    } finally {
      logger.removeAppender(listLogAppender)
    }
    val pdCacheInvalidateListenerLog = listLogAppender.getLog.stream().filter(
      e => e.getLoggerName == "com.pingcap.tispark.listener.PDCacheInvalidateListener").
      collect(Collectors.toList[LoggingEvent])
    assert(pdCacheInvalidateListenerLog.size() == 1)
  }

  class ListLogAppender extends AppenderSkeleton {

    final private val log = new util.ArrayList[LoggingEvent]()

    override def requiresLayout = false

    override protected def append(loggingEvent: LoggingEvent): Unit = {
      log.add(loggingEvent)
    }

    override def close(): Unit = {
    }

    def getLog = new util.ArrayList[LoggingEvent](log)
  }
}
