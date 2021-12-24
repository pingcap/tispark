package com.pingcap.tispark.auth

import com.pingcap.tikv.{TiConfiguration, TiDBJDBCClient}
import com.pingcap.tispark.TiDBUtils
import com.pingcap.tispark.auth.TiAuthorization.{logger, parsePrivilegeFromRow, refreshInterval}
import com.pingcap.tispark.write.TiDBOptions
import org.apache.spark.sql.internal.SQLConf
import org.slf4j.LoggerFactory

import java.sql.SQLException
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.collection.JavaConverters
import scala.util.control.Breaks.{break, breakable}

case class TiAuthorization private (parameters: Map[String, String], tiConf: TiConfiguration) {

  private var tiDBJDBCClient: TiDBJDBCClient = _

  private val scheduler: ScheduledExecutorService =
    Executors.newScheduledThreadPool(1)

  val globalPrivs: AtomicReference[List[MySQLPriv.Value]] =
    new AtomicReference(List())

  val databasePrivs: AtomicReference[Map[String, List[MySQLPriv.Value]]] =
    new AtomicReference(Map())

  val tablePrivs: AtomicReference[Map[String, Map[String, List[MySQLPriv.Value]]]] =
    new AtomicReference(Map())

  val task: Runnable = () => {
    val privs = getPrivileges
    globalPrivs.set(privs.globalPriv)
    databasePrivs.set(privs.databasePrivs)
    tablePrivs.set(privs.tablePrivs)
  }

  /**
   * Initialization
   */
  {
    TiAuthorization.dbPrefix = tiConf.getDBPrefix
    val option = new TiDBOptions(parameters)
    try {
      this.tiDBJDBCClient = new TiDBJDBCClient(TiDBUtils.createConnectionFactory(option.url)())
    } catch {
      case e: Throwable => {
        // If failed to tiDBJDBCClient, which means authentication failed, the spark session should be shutdown
        logger.error(f"Failed to create tidb jdbc client with url ${option.url}", e)
        throw e
      }
    }

    task.run()
    // Periodically update privileges from TiDB
    scheduler.scheduleWithFixedDelay(task, refreshInterval, refreshInterval, TimeUnit.SECONDS)
  }

  def getPrivileges: PrivilegeObject = {
    var input = JavaConverters.asScalaBuffer(tiDBJDBCClient.showGrants).toList

    /** TODO: role-based privilege
     *
     * Show grants using more than two roles return incorrect result
     * https://github.com/pingcap/tidb/issues/30855
     *
     * val roles = extractRoles(input)
     * input =
     * if (roles.nonEmpty)
     * JavaConverters
     * .asScalaBuffer(
     * tiDBJDBCClient.showGrantsUsingRole(roles.asJava)
     * ).toList
     * else input
     */

    parsePrivilegeFromRow(input)
  }

  def getPDAddress(): String = {
    try {
      tiDBJDBCClient.getPDAddress
    } catch {
      case e: Throwable =>
        throw new IllegalArgumentException(
          "Failed to get pdAddress from TiDB, please make sure user has `PROCESS` privilege on `INFORMATION_SCHEMA`.`CLUSTER_INFO`",
          e)
    }
  }

  /**
   * globalPrivs stores privileges of global dimension for the current user.
   * List($globalPrivileges)
   */
  private def checkGlobalPiv(mySQLPriv: MySQLPriv.Value): Boolean = {
    val privs = globalPrivs.get()
    if (privs.contains(MySQLPriv.AllPriv) || privs.contains(mySQLPriv)) true
    else false
  }

  /**
   * databasePrivs stores privileges of database dimension for the current user.
   * Map($databaseName -> List($databasePrivileges))
   */
  private def checkDatabasePiv(db: String, mySQLPriv: MySQLPriv.Value): Boolean = {
    val privs = databasePrivs.get().getOrElse(db, List())
    if (privs.contains(MySQLPriv.AllPriv) || privs.contains(mySQLPriv)) true
    else false
  }

  /**
   * tablePrivs stores privileges of table dimension for the current user.
   * Map($databaseName -> Map($tableName -> List($tablePrivileges)))
   */
  private def checkTablePiv(db: String, table: String, mySQLPriv: MySQLPriv.Value): Boolean = {
    // If tablePrivs not contains the table, it will return an empty privilegeList for the table
    val privs =
      tablePrivs.get().getOrElse(db.trim, Map()).getOrElse(table.trim, List())
    if (privs.contains(MySQLPriv.AllPriv) || privs.contains(mySQLPriv)) true
    else false
  }

  /**
   * Check whether user has the required privilege of database/table
   *
   * @param db    the name of database
   * @param table the name of table, is empty when check for privilege of database
   * @param requiredPriv
   * @return If the check not passes, throw @SQLException
   */
  def checkPrivs(db: String, table: String, requiredPriv: MySQLPriv.Value): Unit = {
    if (!checkGlobalPiv(requiredPriv) && !checkDatabasePiv(db, requiredPriv) && !checkTablePiv(
        db,
        table,
        requiredPriv))
      throw new SQLException(f"Lack of privilege:$requiredPriv on database:$db table:$table")
  }

  /**
   * Check whether the database/table be visible for the user or not
   *
   * @param db    the name of database
   * @param table the name of table, is empty when check for privilege of database
   * @return
   */
  def visible(db: String, table: String): Boolean = {
    // Account who has ShowDBPriv is able to see all databases and tables regardless of revokes.
    if (globalPrivs.get().contains(MySQLPriv.AllPriv) || globalPrivs
        .get()
        .contains(MySQLPriv.ShowDBPriv)) {
      return true
    }

    // Account who has any Priv of the database/table can see the database/table
    if (table.isEmpty) {
      databasePrivs.get().contains(db) || tablePrivs
        .get()
        .contains(db)
    } else {
      databasePrivs.get().contains(db) || tablePrivs
        .get()
        .getOrElse(db, Map())
        .contains(table)
    }

  }
}

case class PrivilegeObject(
    globalPriv: List[MySQLPriv.Value],
    databasePrivs: Map[String, List[MySQLPriv.Value]],
    tablePrivs: Map[String, Map[String, List[MySQLPriv.Value]]]) {}

object TiAuthorization {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * the required conf for initialization.
   * Must be set before the initialization.
   */
  var sqlConf: SQLConf = _
  var tiConf: TiConfiguration = _

  /**
   * lazy global singleton for Authorization
   * Use initTiAuthorization() to init singleton
   */
  private[this] var _tiAuthorization: TiAuthorization = _
  @volatile private var initialized = false
  private final val lock = new ReentrantLock()

  def tiAuthorization: TiAuthorization = {
    if (!initialized) {
      try {
        lock.lock()
        if (!initialized) {
          _tiAuthorization = new TiAuthorization(
            Map(
              "tidb.addr" -> sqlConf.getConfString("spark.sql.tidb.addr"),
              "tidb.port" -> sqlConf.getConfString("spark.sql.tidb.port"),
              "tidb.user" -> sqlConf.getConfString("spark.sql.tidb.user"),
              "tidb.password" -> sqlConf.getConfString("spark.sql.tidb.password"),
              "multiTables" -> "true"),
            tiConf)
          initialized = true
          _tiAuthorization
        } else {
          _tiAuthorization
        }
      } finally {
        lock.unlock()
      }
    } else {
      _tiAuthorization
    }
  }

  val refreshInterval: Int = 10

  var enableAuth: Boolean = false

  // Compatible with feature `spark.tispark.db_prefix`
  var dbPrefix: String = ""

  /** Currently, There are 2 kinds of grant output format in TiDB:
   * - GRANT [grants] ON [db.table] TO [user]
   * - GRANT [roles] TO [user]
   * Examples:
   * - GRANT PROCESS,SHOW DATABASES,CONFIG ON *.* TO 'dashboardAdmin'@'%'
   * - GRANT 'app_read'@'%' TO 'test'@'%'
   *
   * In order to get role's privilege:
   * > SHOW GRANTS FOR ${user} USING ${role};
   */
  private val userGrantPattern =
    "GRANT\\s+(.+)\\s+ON\\s+(\\S+\\.\\S+)\\s+TO.+".r

  def parsePrivilegeFromRow(privStrings: List[String]): PrivilegeObject = {
    var globalPriv: List[MySQLPriv.Value] = List()
    var databasePrivs: Map[String, List[MySQLPriv.Value]] = Map()
    var tablePrivs: Map[String, Map[String, List[MySQLPriv.Value]]] = Map()

    for (elem <- privStrings) {
      breakable {
        val matchResult = userGrantPattern findFirstMatchIn elem
        if (matchResult.isEmpty) {
          break
        }

        // use regex to parse [GRANTS](group1) and [db.table](group2)
        val privs: List[MySQLPriv.Value] = matchResult.get
          .group(1)
          .split(",")
          .map(m => {
            MySQLPriv.Str2Priv(m.trim)
          })
          .toList
        val database: String = matchResult.get.group(2).split("\\.").head
        val table: String = matchResult.get.group(2).split("\\.").last

        // generate privileges store objects
        if (database == "*" && table == "*") {
          globalPriv ++= privs
        } else if (table == "*") {
          databasePrivs += (f"$dbPrefix$database" -> privs)
        } else {
          val prevTable = tablePrivs.getOrElse(f"$dbPrefix$database", Map.empty)
          tablePrivs += (f"$dbPrefix$database" -> (prevTable + (table -> privs)))
        }
      }
    }

    PrivilegeObject(globalPriv, databasePrivs, tablePrivs)
  }

  private val roleGrantPattern = "GRANT\\s+('\\w+'@.+)+TO.+".r
  private val rolePattern = "'(\\w+)'@.+".r

  def extractRoles(privStrings: List[String]): List[String] = {
    for {
      elem: String <- privStrings
      matchResult = roleGrantPattern findFirstMatchIn elem
      if matchResult.isDefined
      roles = matchResult.get.group(1);
      rawRole <- roles.split(",")
      roleMatch = rolePattern findFirstMatchIn rawRole
      if roleMatch.isDefined
      role = roleMatch.get.group(1)
      if role.trim.nonEmpty
    } yield role.trim
  }

  /**
   * Authorization for statement
   */
  def authorizeForSelect(table: String, database: String, tiAuth: TiAuthorization): Unit = {
    tiAuth.checkPrivs(database, table, MySQLPriv.SelectPriv)
  }

  def authorizeForCreateTableLike(
      targetDb: String,
      targetTable: String,
      sourceDb: String,
      sourceTable: String,
      tiAuth: TiAuthorization) = {
    tiAuth.checkPrivs(targetDb, targetTable, MySQLPriv.CreatePriv)
    tiAuth.checkPrivs(sourceDb, sourceTable, MySQLPriv.SelectPriv)
  }

  def authorizeForSetDatabase(database: String, tiAuth: TiAuthorization) = {
    if (!tiAuth.visible(database, "")) {
      throw new SQLException(f"Lack of privilege to set database:${database}")
    }
  }

  def authorizeForDescribeTable(table: String, database: String, tiAuth: TiAuthorization) = {
    if (!tiAuth.visible(database, table))
      throw new SQLException(f"Lack of privilege to describe table:${table}")
  }
}
