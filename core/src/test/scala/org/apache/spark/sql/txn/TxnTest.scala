package org.apache.spark.sql.txn

import java.sql.DriverManager

import org.apache.spark.sql.BaseTiSparkSuite
import org.apache.spark.sql.catalyst.util.resourceToString

class TxnTest extends BaseTiSparkSuite {
  protected final val sumString = resourceToString(
    s"resolveLock-test/sum_account.sql",
    classLoader = Thread.currentThread().getContextClassLoader
  )
  protected final val q1String = resourceToString(
    s"resolveLock-test/q1.sql",
    classLoader = Thread.currentThread().getContextClassLoader
  )
  protected final val q2String = resourceToString(
    s"resolveLock-test/q2.sql",
    classLoader = Thread.currentThread().getContextClassLoader
  )
  protected final val giveString = resourceToString(
    s"resolveLock-test/1_give.sql",
    classLoader = Thread.currentThread().getContextClassLoader
  )
  protected final val getString = resourceToString(
    s"resolveLock-test/2_get.sql",
    classLoader = Thread.currentThread().getContextClassLoader
  )
  protected final val accountString = resourceToString(
    s"resolveLock-test/1_account.sql",
    classLoader = Thread.currentThread().getContextClassLoader
  )
  protected final val rnd = new scala.util.Random

  protected def queryTIDBTxn(query: Seq[String]): Boolean = {
    val jdbcUsername = "root"

    val jdbcPassword = ""

    val conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)

    try {
      //Assume a valid connection object conn//Assume a valid connection object conn

      conn.setAutoCommit(false)
      val stmt = conn.createStatement()

      query.foreach {
        case q =>
          stmt.executeUpdate(q)
      }

      conn.commit()

      return true
    } catch {
      case e =>
        conn.rollback ()
    }
    false
  }

  test("resolveLock concurrent test") {
    spark.conf.set("spark.tispark.request.isolation.level", 0)
    createOrReplaceTempView("resolveLock_test", "CUSTOMER", "")
    var threads = Seq[Thread]()

    for (i <- 1 to 100) {
      val thread = new Thread {
        override def run {
          var ok = true
          while (ok) {
            try {
              val num = rnd.nextInt(600).toString
              val id1 = (1 + rnd.nextInt(150)).toString
              val id2 = (1 + rnd.nextInt(150)).toString
              val querys = Seq[String](
                giveString.replace("$1", num).replace("$2", id1),
                getString.replace("$1", num).replace("$2", id2)
              )
              queryTIDBTxn(querys)
              println("txn " + i.toString + " success!")
              ok = false
            } catch {
              case e =>
                Thread.sleep(1000)
                ok = true
            }
          }
        }
      }
      thread.start()

      threads = threads :+ thread
    }

    println(threads.size)

    threads.foreach {
      t =>
        t.join()
    }

    println(querySpark(sumString))
  }
}
