package com.pingcap.tispark

import java.sql.{Connection, Driver, DriverManager}
import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, DriverWrapper}

import scala.util.Try

object TiDBUtils {
  private val TIDB_DRIVER_CLASS = "com.mysql.jdbc.Driver"

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
   * @param jdbcURL
   */
  def createConnectionFactory(jdbcURL: String): () => Connection = {
    import scala.collection.JavaConverters._
    val driverClass: String = TIDB_DRIVER_CLASS
    () =>
      {
        DriverRegistry.register(driverClass)
        val driver: Driver = DriverManager.getDrivers.asScala
          .collectFirst {
            case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == driverClass => d
            case d if d.getClass.getCanonicalName == driverClass                        => d
          }
          .getOrElse {
            throw new IllegalStateException(
              s"Did not find registered driver with class $driverClass"
            )
          }
        driver.connect(jdbcURL, new Properties())
      }
  }
}
