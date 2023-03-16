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

package com.pingcap.tispark.safepoint

import com.pingcap.tikv.TiSession
import com.pingcap.tikv.exception.TiInternalException
import com.pingcap.tikv.meta.TiTimestamp
import com.pingcap.tikv.util.{BackOffer, ConcreteBackOffer}
import org.slf4j.LoggerFactory

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

case class ServiceSafePoint(
    serviceId: String,
    ttl: Long,
    GCMaxWaitTime: Long,
    tiSession: TiSession) {

  private final val logger = LoggerFactory.getLogger(getClass.getName)
  private var minStartTs = Long.MaxValue
  val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  service.scheduleAtFixedRate(
    () => {
      if (minStartTs != Long.MaxValue) {
        val safePoint = tiSession.getPDClient.updateServiceGCSafePoint(
          serviceId,
          ttl,
          minStartTs,
          ConcreteBackOffer.newCustomBackOff(BackOffer.PD_UPDATE_SAFE_POINT_BACKOFF))
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
      val now = tiSession.getTimestamp
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
    val safePoint = tiSession.getPDClient.updateServiceGCSafePoint(
      serviceId,
      ttl,
      minStartTs,
      ConcreteBackOffer.newCustomBackOff(BackOffer.PD_UPDATE_SAFE_POINT_BACKOFF))
    if (safePoint > startTs) {
      throw new TiInternalException(
        s"Failed to check service GC safe point because the current minimum safe point $safePoint is newer than start_ts $startTs.")
    }
    logger.info(s"check start_ts $startTs success.")
  }

  private def applyServiceSafePoint(startTs: Long): Unit = {
    val safePoint = tiSession.getPDClient.updateServiceGCSafePoint(
      serviceId,
      ttl,
      startTs,
      ConcreteBackOffer.newCustomBackOff(BackOffer.PD_UPDATE_SAFE_POINT_BACKOFF))
    if (safePoint > startTs) {
      throw new TiInternalException(
        s"Failed to register service GC safe point because the current minimum safe point $safePoint is newer than what we assume $startTs.")
    }
    logger.info(s"register service GC safe point $startTs success.")
  }

  def stopRegisterSafePoint(): Unit = {
    minStartTs = Long.MaxValue
    tiSession.getPDClient.updateServiceGCSafePoint(
      serviceId,
      ttl,
      Long.MaxValue,
      ConcreteBackOffer.newCustomBackOff(BackOffer.PD_UPDATE_SAFE_POINT_BACKOFF))
    service.shutdownNow()
  }
}
