package com.pingcap.tispark.safepoint

import com.pingcap.tikv.TiSession
import com.pingcap.tikv.exception.TiInternalException
import com.pingcap.tikv.util.{BackOffer, ConcreteBackOffer}
import org.slf4j.LoggerFactory

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

case class ServiceSafePoint(serviceId: String, ttl: Long, tiSession: TiSession) {

  private final val logger = LoggerFactory.getLogger(getClass.getName)
  private var minStartTs = Long.MaxValue
  val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  service.scheduleAtFixedRate(
    () => {
      if (minStartTs != Long.MaxValue) {
        val safePoint = tiSession.getPDClient.UpdateServiceGCSafePoint(
          serviceId,
          ttl,
          minStartTs,
          ConcreteBackOffer.newCustomBackOff(BackOffer.PD_UPDATE_SAFE_POINT_BACKOFF))
        if (safePoint > minStartTs) {
          // will not happen unless someone delete the TiSpark service safe point in PD compulsively
          logger.error(
            s"Failed to register service GC safe point because the current minimum safe point $safePoint is newer than what we assume $minStartTs. Maybe you delete the TiSpark safe point in PD")
        } else {
          logger.info(s"register service GC safe point $minStartTs success.")
        }
      }
    },
    0,
    1,
    TimeUnit.MINUTES)

  def updateStartTs(startTs: Long): Unit = {
    checkServiceSafePoint(startTs)
    if (startTs < minStartTs) {
      minStartTs = startTs
      checkServiceSafePoint(minStartTs)
    }
  }

  def checkServiceSafePoint(startTs: Long): Unit = {
    val safePoint = tiSession.getPDClient.UpdateServiceGCSafePoint(
      serviceId,
      ttl,
      minStartTs,
      ConcreteBackOffer.newCustomBackOff(BackOffer.PD_INFO_BACKOFF))
    if (safePoint > startTs) {
      throw new TiInternalException(
        s"Failed to register service GC safe point because the current minimum safe point $safePoint is newer than what we assume $startTs")
    }
  }

  def stopRegisterSafePoint(): Unit = {
    service.shutdownNow()
  }
}
