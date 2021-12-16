package com.pingcap.tispark.auth

import com.pingcap.tikv.{TiConfiguration, TiDBJDBCClient}
import com.pingcap.tispark.TiDBUtils
import com.pingcap.tispark.auth.TiAuthorization.{extractRoles, parsePrivilegeFromRow, refreshInterval}
import com.pingcap.tispark.write.TiDBOptions

import java.sql.SQLException
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.collection.JavaConverters
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.util.control.Breaks.{break, breakable}

case class TiAuthorization(parameters: Map[String, String], tiConf:TiConfiguration) {

  private var tiDBJDBCClient: TiDBJDBCClient = _

  private val scheduler: ScheduledExecutorService =
    Executors.newScheduledThreadPool(1)

  val globalPriv: AtomicReference[List[MySQLPriv.Value]] =
    new AtomicReference(List())

  val databasePrivs: AtomicReference[Map[String, List[MySQLPriv.Value]]] =
    new AtomicReference(Map())

  val tablePrivs: AtomicReference[Map[String, List[MySQLPriv.Value]]] =
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
    this.tiDBJDBCClient = new TiDBJDBCClient(
      TiDBUtils.createConnectionFactory(option.url)()
    )

    task.run()
    scheduler.scheduleWithFixedDelay(task, refreshInterval, refreshInterval, TimeUnit.SECONDS)
  }

  def getPrivileges: PrivilegeObject = {
    var input = JavaConverters.asScalaBuffer(tiDBJDBCClient.showGrants).toList
    val roles = extractRoles(input)

    input =
      if (roles.nonEmpty)
        JavaConverters
          .asScalaBuffer(
            tiDBJDBCClient.showGrantsUsingRole(roles.asJava)
          )
          .toList
      else input

    parsePrivilegeFromRow(input)
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

  def checkTablePiv(
      db: String,
      table: String,
      mySQLPriv: MySQLPriv.Value
  ): Boolean = {
    val privs = tablePrivs.get().getOrElse(f"${db.trim}.${table.trim}", List())
    if (privs.contains(MySQLPriv.AllPriv) || privs.contains(mySQLPriv)) true
    else false
  }

  def checkPrivs(
      db: String,
      table: String,
      requiredPriv: MySQLPriv.Value
  ): Unit = {
    if (
      !checkGlobalPiv(requiredPriv) && !checkDatabasePiv(
        db,
        requiredPriv
      ) && !checkTablePiv(db, table, requiredPriv)
    ) throw new SQLException(
        f"Lack of privilege:$requiredPriv on database:$db table:$table"
      )
  }
}

case class PrivilegeObject(
    globalPriv: List[MySQLPriv.Value],
    databasePrivs: Map[String, List[MySQLPriv.Value]],
    tablePrivs: Map[String, List[MySQLPriv.Value]]
) {}

object TiAuthorization {

  val refreshInterval: Int = 10

  private[this] var _enableAuth: Boolean = false

  def enableAuth: Boolean = _enableAuth

  def setEnableAuth(value: Boolean): Unit = {
  _enableAuth = value
  }

  private var dbPrefix:String = ""

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

  def parsePrivilegeFromRow(privStrings: List[String]): PrivilegeObject = {
    var globalPriv: List[MySQLPriv.Value] = List()
    var databasePrivs: Map[String, List[MySQLPriv.Value]] = Map()
    var tablePrivs: Map[String, List[MySQLPriv.Value]] = Map()

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
        val table: String = matchResult.get.group(2).split("\\.").tail.head

        if (database == "*" && table == "*") {
          globalPriv ++= privs
        } else if (table == "*") {
          databasePrivs += ( f"$dbPrefix$database" -> privs)
        } else {
          tablePrivs += (matchResult.get.group(2) -> privs)
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
      roleMatch = "'(\\w+)'@.+".r findFirstMatchIn rawRole
      if roleMatch.isDefined
      role = roleMatch.get.group(1)
      if role.trim.nonEmpty
    } yield role.trim
  }
}
