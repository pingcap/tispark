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

package com.pingcap.tispark

import java.sql.{Connection, Driver, DriverManager}
import java.util.Properties

import com.pingcap.tispark.write.TiDBOptions
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, DriverWrapper}

import scala.util.Try

object TiDBUtils {
  val TIDB_DRIVER_CLASS = "com.mysql.jdbc.Driver"

  /**
   * Returns true if the table already exists in the TiDB.
   */
  def tableExists(conn: Connection, options: TiDBOptions): Boolean = {
    val sql = s"SELECT * FROM `${options.database}`.`${options.table}` WHERE 1=0"
    Try {
      val statement = conn.prepareStatement(sql)
      try {
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

  /**
   * Returns a factory for creating connections to the given TiDB URL.
   *
   * @param jdbcURL jdbc url
   */
  def createConnectionFactory(jdbcURL: String): () => Connection = {
    import scala.collection.JavaConverters._
    val driverClass: String = TIDB_DRIVER_CLASS
    () => {
      DriverRegistry.register(driverClass)
      val driver: Driver = DriverManager.getDrivers.asScala
        .collectFirst {
          case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == driverClass => d
          case d if d.getClass.getCanonicalName == driverClass => d
        }
        .getOrElse {
          throw new IllegalStateException(
            s"Did not find registered driver with class $driverClass")
        }
      driver.connect(jdbcURL, new Properties())
    }
  }
}
