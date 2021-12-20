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

  val globalPriv: AtomicReference[List[MySQLPriv.Value]] =
    new AtomicReference(List())

  val databasePrivs: AtomicReference[Map[String, List[MySQLPriv.Value]]] =
    new AtomicReference(Map())

  val tablePrivs: AtomicReference[Map[String, Map[String, List[MySQLPriv.Value]]]] =
    new AtomicReference(Map())

  val task: Runnable = () => {
    val privs = getPrivileges
    globalPriv.getAndSet(privs.globalPriv)
    databasePrivs.getAndSet(privs.databasePrivs)
    tablePrivs.getAndSet(privs.tablePrivs)
  }

  {
    TiAuthorization.dbPrefix = tiConf.getDBPrefix
    val option = new TiDBOptions(parameters)
    try {
      this.tiDBJDBCClient = new TiDBJDBCClient(TiDBUtils.createConnectionFactory(option.url)())
    } catch {
      case e: Throwable => {
        logger.error(f"Failed to create tidb jdbc client with url ${option.url}", e)
        System.exit(-1)
      }
    }

    task.run()
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

  def checkGlobalPiv(mySQLPriv: MySQLPriv.Value): Boolean = {
    val privs = globalPriv.get()
    if (privs.contains(MySQLPriv.AllPriv) || privs.contains(mySQLPriv)) true
    else false
  }

  def checkDatabasePiv(db: String, mySQLPriv: MySQLPriv.Value): Boolean = {
    val privs = databasePrivs.get().getOrElse(db, List())
    if (privs.contains(MySQLPriv.AllPriv) || privs.contains(mySQLPriv)) true
    else false
  }

  def checkTablePiv(db: String, table: String, mySQLPriv: MySQLPriv.Value): Boolean = {
    val privs =
      tablePrivs.get().getOrElse(db.trim, Map()).getOrElse(table.trim, List())
    if (privs.contains(MySQLPriv.AllPriv) || privs.contains(mySQLPriv)) true
    else false
  }

  def checkPrivs(db: String, table: String, requiredPriv: MySQLPriv.Value): Unit = {
    if (!checkGlobalPiv(requiredPriv) && !checkDatabasePiv(db, requiredPriv) && !checkTablePiv(
        db,
        table,
        requiredPriv))
      throw new SQLException(f"Lack of privilege:$requiredPriv on database:$db table:$table")
  }

  def visible(db: String, table: String): Boolean = {
    // Account who has ShowDBPriv is able to see all databases and tables regardless of revokes.
    if (globalPriv.get().contains(MySQLPriv.AllPriv) || globalPriv
        .get()
        .contains(MySQLPriv.ShowDBPriv)) {
      return true
    }

    // Account who has any Priv of the database/table can see the database/table
    if (table.isEmpty) {
      databasePrivs.get().keySet.contains(db) || tablePrivs
        .get()
        .keySet
        .contains(db)
    } else {
      databasePrivs.get().keySet.contains(db) || tablePrivs
        .get()
        .getOrElse(db, Map())
        .keySet
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

  private final val lock = new ReentrantLock()

  private var initialized = false

  var sqlConf: SQLConf = _

  var tiConf: TiConfiguration = _

  private[this] var _tiAuthorization: TiAuthorization = _

  def tiAuthorization: TiAuthorization = {
    if (initialized) {
      _tiAuthorization
    } else {
      throw new IllegalArgumentException("TiAuthorization has not been initialized")
    }
  }

  def initTiAuthorization(): Unit = {
    lock.lock()
    try {
      if (initialized) {
        logger.warn("TiAuthorization has already been initialized")
      } else {
        _tiAuthorization = new TiAuthorization(
          Map(
            "tidb.addr" -> sqlConf.getConfString("spark.sql.tidb.addr"),
            "tidb.port" -> sqlConf.getConfString("spark.sql.tidb.port"),
            "tidb.user" -> sqlConf.getConfString("spark.sql.tidb.user"),
            "tidb.password" -> sqlConf.getConfString("spark.sql.tidb.password"),
            "multiTables" -> "true"),
          tiConf)
        initialized = true
      }
    } finally {
      lock.unlock()
    }
  }

  val refreshInterval: Int = 10

  var enableAuth: Boolean = false

  var dbPrefix: String = ""

  /**  Currently, There are 2 kinds of grant output format in TiDB:
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
  private val roleGrantPattern = "GRANT\\s+('\\w+'@.+)+TO.+".r
  private val rolePattern = "'(\\w+)'@.+".r

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

        val privs: List[MySQLPriv.Value] = matchResult.get
          .group(1)
          .split(",")
          .map(m => { MySQLPriv.Str2Priv(m.trim) })
          .toList
        val database: String = matchResult.get.group(2).split("\\.").head
        val table: String = matchResult.get.group(2).split("\\.").last

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

  /** Authorization for statement
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
