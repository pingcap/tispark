package com.pingcap.tispark.delete

import com.pingcap.tispark.datasource.BaseBatchWriteTest
import org.scalatest.Matchers.{be, noException}

/**
 * Delete WHERE Clause test. We need to support all WHERE clauses supported by TiDB
 *
 */

class DeleteWhereClauseSuite extends BaseBatchWriteTest("test_delete_where_clause") {

  private val whereClauseInt = Seq[String](
    "i=0",
    "i!=0",
    "i is null",
    "i>0",
    "i<0",
    "i>=0",
    "i<=0",
    "i>0 and i<2",
    "i<0 or i>2",
    "i in (0,1,2)",
    "not i=1")

  private val whereClauseString = Seq[String](
    "s='0'",
    "s!='0'",
    "s is null",
    "s is not null",
    "s>'0'",
    "s<'0'",
    "s>='0'",
    "s<='0'",
    "s>'0' and s<'2'",
    "s<'0' or s>'2'",
    "s like '%1%'",
    "s like '1%'",
    "s like '%1'",
    "s in ('0','1','2')",
    "not s='1'")

  test("Delete WHERE Clause test: int") {
    jdbcUpdate(s"create table $dbtable(i int, s varchar(255),PRIMARY KEY (i))")
    whereClauseInt.foreach(query => noException should be thrownBy judge(query))
  }

  test("Delete WHERE Clause test: String") {
    jdbcUpdate(s"create table $dbtable(i int, s varchar(255),PRIMARY KEY (i))")
    whereClauseString.foreach(query => noException should be thrownBy judge(query))
  }

  def judge(condition: String): Unit = {
    val query = s"delete from $dbtable where $condition"
    try {
      spark.sql(query)
    } catch {
      case e: Throwable => {
        logger.error(s"DeleteWhereClauseSuite fail, query: $query")
        throw e
      }
    }
  }

}
