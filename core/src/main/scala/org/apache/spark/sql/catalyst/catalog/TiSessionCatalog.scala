package org.apache.spark.sql.catalyst.catalog

import java.util.Locale

import org.apache.spark.sql.TiContext
import org.apache.spark.sql.internal.StaticSQLConf

trait TiSessionCatalog extends SessionCatalog {

  protected val tiContext: TiContext

  protected lazy val globalTempDB: String =
    tiContext.sparkSession.conf.get(StaticSQLConf.GLOBAL_TEMP_DATABASE).toLowerCase(Locale.ROOT)

  /**
   * Returns the catalog in which the database is.
   * Use for upper command to identify the catalog in order to take different actions.
   * @param database database
   * @return respective catalog containing this database,
   *        returns current catalog when database is empty
   */
  def catalogOf(database: Option[String] = None): Option[SessionCatalog]
}
