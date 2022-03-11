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
import org.apache.spark.sql.catalyst.analyzer.{TiAuthRuleFactory, TiAuthorizationRule}
import org.apache.spark.sql.catalyst.catalog.TiCatalog
import org.apache.spark.sql.catalyst.planner.TiStrategyFactory
import org.slf4j.LoggerFactory

import scala.collection.mutable

class TiExtensions extends (SparkSessionExtensions => Unit) {
  private val tiContextMap = mutable.HashMap.empty[SparkSession, TiContext]

  override def apply(e: SparkSessionExtensions): Unit = {
    TiSparkInfo.checkVersion()

    e.injectResolutionRule(new TiAuthRuleFactory(getOrCreateTiContext))
    e.injectPlannerStrategy(new TiStrategyFactory(getOrCreateTiContext))
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

  /**
   * Catalog for tidb is necessary now.
   * @param sparkSession
   */
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

  /**
   * Use TiAuthorizationRule to judge if TiExtensions is enable.
   * It needs to be changed if TiAuthorizationRule is deleted
   * @param sparkSession
   * @return
   */
  def getTiContext(sparkSession: SparkSession): Option[TiContext] = {
    val extendedResolutionRules = sparkSession.sessionState.analyzer.extendedResolutionRules
    for (i <- extendedResolutionRules.indices) {
      extendedResolutionRules(i) match {
        case rule: TiAuthorizationRule =>
          return Some(rule.getOrCreateTiContext(sparkSession))
        case _ =>
      }
    }
    None
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
