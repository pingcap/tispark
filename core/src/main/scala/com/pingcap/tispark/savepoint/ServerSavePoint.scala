package com.pingcap.tispark.savepoint

import com.pingcap.tikv.TiSession
import com.pingcap.tikv.exception.TiInternalException
import com.pingcap.tikv.util.{BackOffer, ConcreteBackOffer}
import org.slf4j.LoggerFactory

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

case class ServerSavePoint(serverId: String, ttl: Long, tiSession: TiSession) {

  private final val logger = LoggerFactory.getLogger(getClass.getName)
  private var minStartTs = Long.MaxValue
  val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  service.scheduleAtFixedRate(
    () => {
      if (minStartTs != Long.MaxValue) {
        val savePoint = tiSession.getPDClient.UpdateServiceGCSafePoint(
          serverId,
          ttl,
          minStartTs,
          ConcreteBackOffer.newCustomBackOff(BackOffer.PD_UPDATE_SAFE_POINT_BACKOFF))
        if (savePoint > minStartTs) {
          // will not happen unless someone delete the TiSpark server safe point in PD compulsively
          logger.error(
            s"Failed to register server GC safe point because the current minimum safe point $savePoint is newer than what we assume $minStartTs. Maybe you delete the TiSpark safe point in PD")
        } else {
          logger.info(s"register server GC safe point $minStartTs success.")
        }
      }
    },
    0,
    1,
    TimeUnit.MINUTES)

  def updateStartTs(starTs: Long): Unit = {
    checkServerSafePoint(starTs)
    if (starTs < minStartTs) {
      minStartTs = starTs
    }
  }

  def checkServerSafePoint(starTs: Long): Unit = {
    val savePoint = tiSession.getPDClient.UpdateServiceGCSafePoint(
      serverId,
      ttl,
      minStartTs,
      ConcreteBackOffer.newCustomBackOff(BackOffer.PD_INFO_BACKOFF))
    if (savePoint > starTs) {
      throw new TiInternalException(
        s"Failed to register server GC safe point because the current minimum safe point $savePoint is newer than what we assume $starTs")
    }
  }

  def stopRegisterSavePoint(): Unit = {
    service.shutdownNow()
  }
}
