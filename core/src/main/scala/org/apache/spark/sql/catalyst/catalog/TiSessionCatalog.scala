package org.apache.spark.sql.catalyst.catalog

trait TiSessionCatalog extends SessionCatalog {

  /**
   * Returns the catalog in which the database is.
   * Use for upper command to identify the catalog in order to take different actions.
   * @param database
   * @return
   */
  def catalogOf(database: Option[String] = None): Option[SessionCatalog]
}
