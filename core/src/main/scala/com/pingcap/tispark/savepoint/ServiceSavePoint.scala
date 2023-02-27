package com.pingcap.tispark.savepoint

import com.pingcap.tikv.TiSession
import com.pingcap.tikv.exception.TiInternalException
import com.pingcap.tikv.util.{BackOffer, ConcreteBackOffer}
import org.slf4j.LoggerFactory

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

case class ServiceSavePoint(serviceId: String, ttl: Long, tiSession: TiSession) {

  private final val logger = LoggerFactory.getLogger(getClass.getName)
  private var minStartTs = Long.MaxValue
  val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  service.scheduleAtFixedRate(
    () => {
      if (minStartTs != Long.MaxValue) {
        val savePoint = tiSession.getPDClient.UpdateServiceGCSafePoint(
          serviceId,
          ttl,
          minStartTs,
          ConcreteBackOffer.newCustomBackOff(BackOffer.PD_UPDATE_SAFE_POINT_BACKOFF))
        if (savePoint > minStartTs) {
          // will not happen unless someone delete the TiSpark service safe point in PD compulsively
          logger.error(
            s"Failed to register service GC safe point because the current minimum safe point $savePoint is newer than what we assume $minStartTs. Maybe you delete the TiSpark safe point in PD")
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
    }
  }

  def checkServiceSafePoint(startTs: Long): Unit = {
    val savePoint = tiSession.getPDClient.UpdateServiceGCSafePoint(
      serviceId,
      ttl,
      minStartTs,
      ConcreteBackOffer.newCustomBackOff(BackOffer.PD_INFO_BACKOFF))
    if (savePoint > startTs) {
      throw new TiInternalException(
        s"Failed to register service GC safe point because the current minimum safe point $savePoint is newer than what we assume $starTs")
    }
  }

  def stopRegisterSavePoint(): Unit = {
    service.shutdownNow()
  }
}
