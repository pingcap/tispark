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
   * Creates a table with a given schema.
   */
  def createTable(conn: Connection,
                  df: DataFrame,
                  options: TiDBOptions,
                  tiContext: TiContext): Unit = {
    val strSchema = JdbcUtils.schemaString(df, options.url, options.createTableColumnTypes)
    val createTableOptions = options.createTableOptions
    // Create the table if the table does not exist.
    // To allow certain options to append when create a new table, which can be
    // table_options or partition_options.
    // E.g., "CREATE TABLE t (name string) ENGINE=InnoDB DEFAULT CHARSET=utf8"
    val sql = s"CREATE TABLE ${options.dbtable} ($strSchema) $createTableOptions"
    val statement = conn.createStatement
    try {
      statement.executeUpdate(sql)
    } finally {
      statement.close()
    }

    // reload all meta
    tiContext.meta.reloadAllMeta()
  }

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
   * Save DataFrame to TiDB
   */
  def saveTable(tiContext: TiContext,
                df: DataFrame,
                tableSchema: Option[StructType],
                options: TiDBOptions): Unit =
    // TODO: use table schema
    TiBatchWrite.writeToTiDB(df.rdd, tiContext, options)

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

  /**
   * Truncates a table from TiDB without side effects.
   */
  def truncateTable(conn: Connection, options: TiDBOptions, tiContext: TiContext): Unit = {
    val sql = s"TRUNCATE TABLE ${options.dbtable}"
    val statement = conn.createStatement
    try {
      statement.executeUpdate(sql)
    } finally {
      statement.close()
    }

    // reload all meta
    tiContext.meta.reloadAllMeta()
  }

  /**
   * Returns the schema if the table already exists in TiDB.
   */
  def getSchemaOption(conn: Connection, options: TiDBOptions): Option[StructType] = {
    val dialect = JdbcDialects.get(options.url)
    try {
      val statement = conn.prepareStatement(dialect.getSchemaQuery(options.dbtable))
      try {
        Some(JdbcUtils.getSchema(statement.executeQuery(), dialect))
      } catch {
        case _: SQLException => None
      } finally {
        statement.close()
      }
    } catch {
      case _: SQLException => None
    }
  }

  /**
   * Drops a table from TiDB.
   */
  def dropTable(conn: Connection, options: TiDBOptions, tiContext: TiContext): Unit = {
    val statement = conn.createStatement
    try {
      statement.executeUpdate(s"DROP TABLE ${options.dbtable}")
    } finally {
      statement.close()
    }

    // reload all meta
    tiContext.meta.reloadAllMeta()
  }

  def isCascadingTruncateTable(url: String): Option[Boolean] =
    JdbcDialects.get(url).isCascadingTruncateTable()
}
