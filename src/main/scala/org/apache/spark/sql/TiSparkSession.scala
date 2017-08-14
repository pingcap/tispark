package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionState, SharedState}
import org.apache.spark.util.Utils

import scala.util.control.NonFatal


class TiSparkSession(@transient private val sparkContext: SparkContext) extends SparkSession(sparkContext) {

  @transient
  override lazy val sharedState: SharedState = new SharedState(sparkContext)

  @transient
  lazy val sessionState: SessionState = {
    instantiateSessionState(
      SparkSession.sessionStateClassName(sparkContext.conf),
      this)
  }

  /**
    * Helper method to create an instance of `SessionState` based on `className` from conf.
    * The result is either `SessionState` or a Hive based `SessionState`.
    */
  private def instantiateSessionState(className: String,
                                      sparkSession: SparkSession): SessionState = {
    try {
      // invoke `new [Hive]SessionStateBuilder(SparkSession, Option[SessionState])`
      val clazz = Utils.classForName(className)
      val ctor = clazz.getConstructors.head
      ctor.newInstance(sparkSession, None).asInstanceOf[BaseSessionStateBuilder].build()
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }
}
