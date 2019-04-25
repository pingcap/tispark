package org.apache.spark.sql.catalyst.plans.logical

import com.pingcap.tikv.meta.TiTimestamp
import com.pingcap.tispark.TiDBRelation
import org.apache.spark.sql.BaseTiSparkSuite
import org.apache.spark.sql.catalyst.expressions.{Exists, ListQuery, ScalarSubquery}
import org.apache.spark.sql.execution.datasources.LogicalRelation

class LogicalPlanTestSuite extends BaseTiSparkSuite {

  test("test timestamp in logical plan") {
    tidbStmt.execute("DROP TABLE IF EXISTS `test1`")
    tidbStmt.execute("DROP TABLE IF EXISTS `test2`")
    tidbStmt.execute("DROP TABLE IF EXISTS `test3`")
    tidbStmt.execute(
      "CREATE TABLE `test1` (`id` int primary key, `c1` int, `c2` int, KEY idx(c1, c2))"
    )
    tidbStmt.execute("CREATE TABLE `test2` (`id` int, `c1` int, `c2` int)")
    tidbStmt.execute("CREATE TABLE `test3` (`id` int, `c1` int, `c2` int, KEY idx(c2))")
    tidbStmt.execute(
      "insert into test1 values(1, 2, 3), /*(1, 3, 2), */(2, 2, 4), (3, 1, 3), (4, 2, 1)"
    )
    tidbStmt.execute(
      "insert into test2 values(1, 2, 3), (1, 2, 4), (2, 1, 4), (3, 1, 3), (4, 3, 1)"
    )
    tidbStmt.execute(
      "insert into test3 values(1, 2, 3), (2, 1, 3), (2, 1, 4), (3, 2, 3), (4, 2, 1)"
    )
    refreshConnections()
    val df =
      spark.sql("""
                  |select t1.*, (
                  |	select count(*)
                  |	from test2
                  |	where id > 1
                  |), t1.c1, t2.c1, t3.*, t4.c3
                  |from (
                  |	select id, c1, c2
                  |	from test1) t1
                  |left join (
                  |	select id, c1, c2, c1 + coalesce(c2 % 2) as c3
                  |	from test2 where c1 + c2 > 3) t2
                  |on t1.id = t2.id
                  |left join (
                  |	select max(id) as id, min(c1) + c2 as c1, c2, count(*) as c3
                  |	from test3
                  |	where c2 <= 3 and exists (
                  |		select * from (
                  |			select id as c1 from test3)
                  |    where (
                  |      select max(id) from test1) = 4)
                  |	group by c2) t3
                  |on t1.id = t3.id
                  |left join (
                  |	select max(id) as id, min(c1) as c1, max(c1) as c1, count(*) as c2, c2 as c3
                  |	from test3
                  |	where id not in (
                  |		select id
                  |		from test1
                  |		where c2 > 2)
                  |	group by c2) t4
                  |on t1.id = t4.id
      """.stripMargin)

    var v: Option[TiTimestamp] = None
    def check(version: Option[TiTimestamp]): Unit =
      if (version.isEmpty) {
        fail("timestamp is not defined!")
      } else if (v.isEmpty) {
        println("initialize timestamp should be " + version.get.getVersion)
        v = version
      } else if (v.get.getVersion != version.get.getVersion) {
        fail("multiple timestamp found in plan")
      } else {
        println("check ok " + v.get.getVersion)
      }

    def checkTimestamp: PartialFunction[LogicalPlan, Unit] = {
      case _ @LogicalRelation(r: TiDBRelation, _, _, _) =>
        check(r.ts)
      case _ @Filter(condition, _) =>
        condition foreach {
          case _ @Exists(p, _, _) =>
            p.foreach(checkTimestamp)
          case _ @ListQuery(p, _, _, _) =>
            p.foreach(checkTimestamp)
          case _ @ScalarSubquery(p, _, _) =>
            p.foreach(checkTimestamp)
          case _ =>
        }
      case _ @Project(projectList, _) =>
        projectList foreach { u =>
          u foreach {
            case _ @ScalarSubquery(p, _, _) =>
              p.foreach(checkTimestamp)
            case _ =>
          }
        }
      case _ =>
    }
    println(df.queryExecution.analyzed)
    df.queryExecution.analyzed.foreach { checkTimestamp }
    df.show
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists test1")
      tidbStmt.execute("drop table if exists test2")
      tidbStmt.execute("drop table if exists test3")
    } finally {
      super.afterAll()
    }
}
