package org.apache.spark.sql.txn

import java.sql.DriverManager

import com.pingcap.tikv.kvproto.Kvrpcpb.IsolationLevel
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

  protected def queryTIDBTxn(query: Seq[String]): Unit = {
    val conn = DriverManager.getConnection(jdbcUrl, "root", "")
    try {
      //Assume a valid connection object conn
      conn.setAutoCommit(false)
      val stmt = conn.createStatement()
      query.foreach {
        case q => {
          stmt.executeUpdate(q)
          Thread.sleep(rnd.nextInt(2000))
        }
      }
      conn.commit()
    } catch {
      case e => {
        conn.rollback()
        throw e
      }
    }
  }

  protected def queryTIDBTxnWithPercentage(query: Seq[String], per: Double, id: String): Unit = {
    val conn = DriverManager.getConnection(jdbcUrl, "root", "")

    try {
      //Assume a valid connection object conn
      conn.setAutoCommit(false)
      val stmt = conn.createStatement()
      val res = stmt.executeQuery(accountString.replace("$1", id))
      val ans = toOutput(res.getObject(1), res.getMetaData.getColumnTypeName(1)).asInstanceOf[Int]
      query.foreach {
        case q => {
          stmt.executeUpdate(q.replace("$1", (ans * per).toString))
          Thread.sleep(rnd.nextInt(2000))
        }
      }
      conn.commit()
    } catch {
      case e =>
        conn.rollback ()
        throw e
    }
  }

  test("resolveLock concurrent test") {
    ti.tiConf.setIsolationLevel(IsolationLevel.SI)
    var threads = Seq[Thread]()
    setCurrentDatabase("resolveLock_test")

    val start = queryTiDB(sumString).head.head

    for (i <- 1 to 100) {
      val thread = new Thread {
        override def run {
          var ok = true
          while (ok) {
            try {
              querySpark(q1String)
              logger.info("query " + i.toString + " success!")
              ok = false
            } catch {
              case e =>
                Thread.sleep(1000 + rnd.nextInt(10000))
                ok = true
            }
          }
        }
      }

      threads = threads :+ thread
    }

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
              logger.info("txn " + i.toString + " success!")
              ok = false
            } catch {
              case e =>
                Thread.sleep(1000 + rnd.nextInt(10000))
            }
          }
        }
      }

      threads = threads :+ thread
    }

    for (i <- 1 to 100) {
      val thread = new Thread {
        override def run {
          var ok = true
          while (ok) {
            try {
              querySpark(q2String)
              logger.info("query " + i.toString + " success!")
            } catch {
              case e =>
                Thread.sleep(1000 + rnd.nextInt(10000))
                ok = true
            }
          }
        }
      }

      threads = threads :+ thread
    }

    for (i <- 1 to 100) {
      val thread = new Thread {
        override def run {
          var ok = true
          while (ok) {
            try {
              val id1 = (1 + rnd.nextInt(150)).toString
              val id2 = (1 + rnd.nextInt(150)).toString
              val querys = Seq[String](
                giveString.replace("$2", id1),
                getString.replace("$2", id2)
              )
              queryTIDBTxnWithPercentage(querys, 0.5, id1)
              logger.info("txn " + i.toString + " success!")
              ok = false
            } catch {
              case e =>
                Thread.sleep(1000 + rnd.nextInt(10000))
                ok = true
            }
          }
        }
      }
      threads = threads :+ thread
    }

    threads = scala.util.Random.shuffle(threads)

    threads.foreach {
      t =>
        t.start()
    }

    threads.foreach {
      t =>
        t.join()
    }

    val end = queryTiDB(sumString).head.head
    if (start != end) {
      fail(
        s"""Failed With
           | error transaction
           | lost or more balance
           | with start $start
           | with end $end
         """.stripMargin)
    }
  }
}
