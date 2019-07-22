package com.pingcap.tispark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, TiContext, TiExtensions, TiStrategy}

import scala.collection.mutable

/** Connector utils, including what needs to be invoked to enable pushdowns. */
object TiSparkConnectorUtils extends Logging {

  private val sessionToContextMap: mutable.HashMap[SparkSession, TiContext] = mutable.HashMap()

  def checkVersionAndEnablePushdown(session: SparkSession, tiContext: TiContext): Boolean = {
    val tiExtensionsEnabled = TiExtensions.enabled()
    if (tiExtensionsEnabled) {
      logWarning("TiExtensions already enabled! Do not need to enable push down!")
    }

    if (!TiSparkInfo.versionSupport()) {
      logWarning(
        s"Spark version ${session.version} does not support push down! " +
          s"Only ${TiSparkInfo.SUPPORTED_SPARK_VERSION.mkString(",")} support push down."
      )
    }

    // put tiContext into map, this will be used later when
    // create tiStrategy.
    sessionToContextMap.put(session, tiContext)
    if (TiSparkInfo.versionSupport() && !tiExtensionsEnabled) {
      enablePushdownSession(session)
      true
    } else {
      disablePushdownSession(session)
      false
    }
  }

  /** Enable more advanced query pushdowns to TiDB.
   *
   * @param session The SparkSession for which pushdowns are to be enabled.
   */
  private def enablePushdownSession(session: SparkSession): Unit =
    if (!session.experimental.extraStrategies.exists(s => s.isInstanceOf[TiStrategy])) {
      session.experimental.extraStrategies ++= Seq(TiStrategy(getOrCreateTiContext)(session))
    }

  private def getOrCreateTiContext(sparkSession: SparkSession): TiContext =
    sessionToContextMap.get(sparkSession) match {
      case Some(tiContext) => tiContext
      case None =>
        val tiContext = new TiContext(sparkSession)
        sessionToContextMap.put(sparkSession, tiContext)
        tiContext
    }

  /** Disable more advanced query pushdowns to TiDB.
   *
   * @param session The SparkSession for which pushdowns are to be disabled.
   */
  private def disablePushdownSession(session: SparkSession): Unit =
    session.experimental.extraStrategies = session.experimental.extraStrategies
      .filterNot(strategy => strategy.isInstanceOf[TiStrategy])
}
