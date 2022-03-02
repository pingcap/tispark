/*
 * Copyright 2020 PingCAP, Inc.
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

package org.apache.spark.sql

import com.pingcap.tikv.exception.TiInternalException
import com.pingcap.tispark.TiSparkInfo
import org.apache.spark.sql.catalyst.catalog.TiCatalog
import org.apache.spark.sql.extensions.{TiAuthRuleFactory, TiResolutionRuleFactory}
import org.slf4j.LoggerFactory

import scala.collection.mutable

class TiExtensions extends (SparkSessionExtensions => Unit) {
  private val tiContextMap = mutable.HashMap.empty[SparkSession, TiContext]

  override def apply(e: SparkSessionExtensions): Unit = {
    TiSparkInfo.checkVersion()

    e.injectResolutionRule(new TiAuthRuleFactory(getOrCreateTiContext))
    e.injectResolutionRule(new TiResolutionRuleFactory(getOrCreateTiContext))
    e.injectPlannerStrategy(TiStrategy(getOrCreateTiContext))
  }

  // call from pyspark only
  def getOrCreateTiContext(sparkSession: SparkSession): TiContext =
    synchronized {
      tiContextMap.get(sparkSession) match {
        case Some(tiContext) => tiContext
        case None =>
          // TODO: make Meta and RegionManager independent to sparkSession
          val tiContext = new TiContext(sparkSession)
          tiContextMap.put(sparkSession, tiContext)
          tiContext
      }
    }
}

object TiExtensions {
  private final val logger = LoggerFactory.getLogger(getClass.getName)
  def authEnable(sparkSession: SparkSession): Boolean = {
    sparkSession.sparkContext.conf
      .get("spark.sql.auth.enable", "false")
      .toBoolean
  }

  def enabled(sparkSession: SparkSession): Boolean = getTiContext(sparkSession).isDefined

  def validateCatalog(sparkSession: SparkSession): Unit = {
    sparkSession.sparkContext.conf
      .getAllWithPrefix("spark.sql.catalog.")
      .toSeq
      .find(pair => TiCatalog.className.equals(pair._2)) match {
      case None =>
        logger.error("TiSpark must work with TiCatalog. Please add TiCatalog in spark conf.")
        throw new TiInternalException(
          "TiSpark must work with TiCatalog. Please add TiCatalog in spark conf.")
      case _ =>
    }
  }

  def getTiContext(sparkSession: SparkSession): Option[TiContext] = {
    if (sparkSession.sessionState.planner.extraPlanningStrategies.nonEmpty &&
      sparkSession.sessionState.planner.extraPlanningStrategies.head
        .isInstanceOf[TiStrategy]) {
      Some(
        sparkSession.sessionState.planner.extraPlanningStrategies.head
          .asInstanceOf[TiStrategy]
          .getOrCreateTiContext(sparkSession))
    } else if (sparkSession.experimental.extraStrategies.nonEmpty &&
      sparkSession.experimental.extraStrategies.head.isInstanceOf[TiStrategy]) {
      Some(
        sparkSession.experimental.extraStrategies.head
          .asInstanceOf[TiStrategy]
          .getOrCreateTiContext(sparkSession))
    } else {
      None
    }
  }

  // call from pyspark only
  private var tiExtensions: TiExtensions = _

  // call from pyspark only
  def getInstance(sparkSession: SparkSession): TiExtensions = {
    if (tiExtensions == null) {
      synchronized {
        if (tiExtensions == null) {
          tiExtensions = new TiExtensions
          tiExtensions.apply(sparkSession.extensions)
        }
      }
    }
    tiExtensions
  }

}
