/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.Litsener

import org.apache.log4j.Logger
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerStageCompleted}

class PDCacheInvalidateListener extends SparkListener {
  protected val logger: Logger = Logger.getLogger(this.getClass)

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    logger.info("Job started.")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    logger.info("Job ended.")
  }
}
