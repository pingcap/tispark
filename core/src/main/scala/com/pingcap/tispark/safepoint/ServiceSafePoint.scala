/*
 * Copyright 2023 PingCAP, Inc.
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
 */

package com.pingcap.tispark.safepoint

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.pingcap.tikv.ClientSession
import org.slf4j.LoggerFactory
import org.tikv.common.exception.TiInternalException
import org.tikv.common.meta.TiTimestamp
import org.tikv.common.util.ConcreteBackOffer

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

case class ServiceSafePoint(
    serviceId: String,
    ttl: Long,
    GCMaxWaitTime: Long,
    clientSession: ClientSession) {

  private val PD_UPDATE_SAFE_POINT_BACKOFF: Int = 20 * 1000
  private final val logger = LoggerFactory.getLogger(getClass.getName)
  private var minStartTs = Long.MaxValue
  val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
    new ThreadFactoryBuilder().setNameFormat("serviceSafePoint-thread-%d").setDaemon(true).build)
  service.scheduleAtFixedRate(
    () => {
      if (minStartTs != Long.MaxValue) {
        val safePoint = clientSession.getTiKVSession.getPDClient.updateServiceGCSafePoint(
          serviceId,
          ttl,
          minStartTs,
          ConcreteBackOffer.newCustomBackOff(PD_UPDATE_SAFE_POINT_BACKOFF))
        if (safePoint > minStartTs) {
          // will not happen unless someone delete the TiSpark service safe point in PD compulsively
          logger.error(
            s"Failed to register service GC safe point because the current minimum safe point $safePoint is newer than what we assume $minStartTs. Maybe you delete the TiSpark safe point in PD.")
        } else {
          logger.info(s"register service GC safe point $minStartTs success.")
        }
      }
    },
    0,
    1,
    TimeUnit.MINUTES)

  // TiSpark can only decrease minStartTs now. Because we can not known which transaction is finished, so we can not increase minStartTs.
  def updateStartTs(startTimeStamp: TiTimestamp): Unit = {
    this.synchronized {
      val now = clientSession.getTiKVSession.getTimestamp
      if (now.getPhysical - startTimeStamp.getPhysical >= GCMaxWaitTime * 1000) {
        throw new TiInternalException(
          s"Can not pause GC more than spark.tispark.gc_max_wait_time=$GCMaxWaitTime s. start_ts: ${startTimeStamp.getVersion}, now: ${now.getVersion}. You can adjust spark.tispark.gc_max_wait_time to increase the gc max wait time.")
      }
      val startTs = startTimeStamp.getVersion
      if (startTs >= minStartTs) {
        // minStartTs >= safe point, so startTs must >= safe point. Check it in case some one delete the TiSpark service safe point in PD compulsively.
        checkServiceSafePoint(startTs)
      } else {
        // applyServiceSafePoint may throw exception. Consider startTs < safePoint < minStartTs.
        applyServiceSafePoint(startTs)
        // let minStartTs = startTs after applyServiceSafePoint success
        minStartTs = startTs
      }
    }
  }

  private def checkServiceSafePoint(startTs: Long): Unit = {
    val safePoint = clientSession.getTiKVSession.getPDClient.updateServiceGCSafePoint(
      serviceId,
      ttl,
      minStartTs,
      ConcreteBackOffer.newCustomBackOff(PD_UPDATE_SAFE_POINT_BACKOFF))
    if (safePoint > startTs) {
      throw new TiInternalException(
        s"Failed to check service GC safe point because the current minimum safe point $safePoint is newer than start_ts $startTs.")
    }
    logger.info(s"check start_ts $startTs success.")
  }

  private def applyServiceSafePoint(startTs: Long): Unit = {
    val safePoint = clientSession.getTiKVSession.getPDClient.updateServiceGCSafePoint(
      serviceId,
      ttl,
      startTs,
      ConcreteBackOffer.newCustomBackOff(PD_UPDATE_SAFE_POINT_BACKOFF))
    if (safePoint > startTs) {
      throw new TiInternalException(
        s"Failed to register service GC safe point because the current minimum safe point $safePoint is newer than what we assume $startTs.")
    }
    logger.info(s"register service GC safe point $startTs success.")
  }

  def stopRegisterSafePoint(): Unit = {
    try {
      minStartTs = Long.MaxValue
      clientSession.getTiKVSession.getPDClient.updateServiceGCSafePoint(
        serviceId,
        ttl,
        Long.MaxValue,
        ConcreteBackOffer.newCustomBackOff(PD_UPDATE_SAFE_POINT_BACKOFF))
    } catch {
      case e: Exception => logger.error("Failed to stop register service GC safe point", e)
    } finally {
      service.shutdownNow()
    }
  }
}
