package com.pingcap.tispark

import java.sql.{Connection, Driver, DriverManager, SQLException}
import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, DriverWrapper, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, TiContext}

import scala.util.Try

object TiDBUtils {

  private val TIDB_DRIVER_CLASS = "com.mysql.jdbc.Driver"

  /**
   * Returns true if the table already exists in the TiDB.
   */
  def tableExists(conn: Connection, options: TiDBOptions): Boolean = {
    val sql = s"SELECT * FROM ${options.dbtable} WHERE 1=0"
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
   * @param options - TiDB options that contains url, table and other information.
   */
  def createConnectionFactory(options: TiDBOptions): () => Connection = {
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
        driver.connect(options.url, new Properties())
      }
  }
}
