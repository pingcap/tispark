/*
 *
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.pingcap.tispark.accumulator;

import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{AppenderSkeleton, Logger}
import org.apache.spark.sql.BaseTiSparkTest

import java.util
import java.util.stream.Collectors;

class AccumulatorSuite extends BaseTiSparkTest {
  test("cacheInvalidateCallback does not work") {
    val listLogAppender = new ListLogAppender
    val logger = Logger.getRootLogger
    logger.addAppender(listLogAppender)
    try {
      tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
      tidbStmt.execute("""
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
    val pdCacheInvalidateListenerLog = listLogAppender.getLog
      .stream()
      .filter(e => e.getLoggerName == "com.pingcap.tispark.listener.PDCacheInvalidateListener")
      .collect(Collectors.toList[LoggingEvent])
    assert(pdCacheInvalidateListenerLog.size() == 1)
  }

  class ListLogAppender extends AppenderSkeleton {

    final private val log = new util.ArrayList[LoggingEvent]()

    override def requiresLayout = false

    override protected def append(loggingEvent: LoggingEvent): Unit = {
      log.add(loggingEvent)
    }

    override def close(): Unit = {}

    def getLog = new util.ArrayList[LoggingEvent](log)
  }
}
