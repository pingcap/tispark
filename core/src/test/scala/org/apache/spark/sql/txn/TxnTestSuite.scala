package org.apache.spark.sql.txn

import java.sql.{DriverManager, SQLException}
import org.tikv.kvproto.Kvrpcpb.IsolationLevel
import org.apache.spark.sql.BaseTiSparkSuite
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.test.Utils.getOrElse
import org.apache.spark.sql.test.TestConstants.{TiDB_PASSWORD, TiDB_USER}

// TODO: this test is not so useful at all
// what I do is to construct a very long-running write operation
// , a very long-running read operation and corresponding
// two quick write and read operation run multiple times(20-100)
// concurrently with multithreading. While the txns' prewrite
// and commit finishes very quickly and almost synchronously.
// What makes the resolveLock almost doesn't happen. What I think
// the most useful way is to implement a mock kv, which delay the time
// between prewrite and commit which cause the txn to rollback
class TxnTestSuite extends BaseTiSparkSuite {
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

  /**
   * query to tidb with jdbc and txn style
   *
   * @param query all queries run in the txn
   * @param wait whether wait every 2 seconds
   * @return Unit
   */
  protected def queryTIDBTxn(query: Seq[String], wait: Boolean): Unit = {
    val jdbcUsername = getOrElse(paramConf(), TiDB_USER, "root")

    val jdbcPassword = getOrElse(paramConf(), TiDB_PASSWORD, "")

    val conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
    try {
      //Assume a valid connection object conn
      conn.setAutoCommit(false)
      val stmt = conn.createStatement()
      query.foreach { q: String =>
        stmt.executeUpdate(q)
        if (wait)
          Thread.sleep(2000)
      }
      conn.commit()
    } catch {
      case e: SQLException =>
        logger.info("rollback1" + e.getMessage)
        conn.rollback()
        throw e
    }
  }

  /**
   * get Spark query thread using corresponding doQuery operation
   *
   * @param i the query number
   * @param doQuery the query operation
   * @return thread
   */
  protected def doThread(i: Int, doQuery: => Unit): Thread =
    new Thread {
      override def run() {
        while (try {
                 doQuery
                 logger.info("query " + i.toString + " success!")
                 false
               } catch {
                 case _: SQLException =>
                   Thread.sleep(1000 + rnd.nextInt(3000))
                   true
               }) {}
      }
    }

  test("resolveLock concurrent test") {
    ti.tiConf.setIsolationLevel(IsolationLevel.SI)

    val start = querySpark(sumString).head.head

    val threads =
      scala.util.Random.shuffle(
        (0 to 239).map(
          i => {
            i / 100 match {
              case 0 =>
                doThread(i, () => {
                  querySpark(q1String)
                })
              case 1 =>
                doThread(
                  i,
                  () => {
                    val num = rnd.nextInt(600).toString
                    val id1 = (1 + rnd.nextInt(150)).toString
                    val id2 = (1 + rnd.nextInt(150)).toString
                    val queries = Seq[String](
                      giveString.replace("$1", num).replace("$2", id1),
                      getString.replace("$1", num).replace("$2", id2)
                    )
                    queryTIDBTxn(queries, wait = true)
                  }
                )
              case 2 =>
                (i - 200) / 20 match {
                  case 0 =>
                    doThread(i, () => {
                      querySpark(q2String)
                    })
                  case 1 =>
                    doThread(
                      i,
                      () => {
                        val array = (1 to 100).map(
                          _ => {
                            val num = rnd.nextInt(600)
                            val id1 = (1 + rnd.nextInt(150)).toString
                            val id2 = (1 + rnd.nextInt(150)).toString
                            (
                              giveString.replace("$1", num.toString).replace("$2", id1),
                              getString.replace("$1", num.toString).replace("$2", id2)
                            )
                          }
                        )

                        val queries = array.map(_._1) ++ array.map(_._2)

                        queryTIDBTxn(queries, wait = false)
                      }
                    )
                }
            }
          }
        )
      )

    assert(threads.size == 240)

    threads.foreach { t =>
      t.start()
    }

    threads.foreach { t =>
      t.join()
    }

    val end = querySpark(sumString).head.head
    if (start != end) {
      fail(s"""Failed With
              | error transaction
              | lost or more balance
              | with start $start
              | with end $end
         """.stripMargin)
    }
  }
}
