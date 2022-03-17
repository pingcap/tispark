package com.pingcap.tispark.delete

import com.pingcap.tispark.datasource.BaseBatchWriteTest

/**
 *  This suite need to pass both tidb 4.x and 5.x
 *  5.x: test cluster index
 *  4.x: this suite can't test all cases because the config `alter-primary-key` can't be changed online.
 *
 */
class DeleteCompatibilitySuite extends BaseBatchWriteTest("test_delete_compatibility") {

  test("Delete cluster index table with int pk (pkIsHandle)") {
    jdbcUpdate(
      s"create table $dbtable(i int, s int,PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(s"insert into $dbtable values(0, 0),(1,1),(2,2),(3,3)")

    spark.sql(s"delete from $dbtable where i >= 2")

    val expected = spark.sql(s"select count(*) from $dbtable where i < 2").head().get(0)
    val actual = spark.sql(s"select count(*) from $dbtable").head().get(0)
    assert(expected == actual)
  }

  test("Delete cluster index table with varchar pk (commonIsHandle)") {
    jdbcUpdate(
      s"create table $dbtable(i varchar(64), s int,PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(s"insert into $dbtable values('0',0),('1',1),('2',2),('3',3)")

    spark.sql(s"delete from $dbtable where i >= '2'")

    val expected = spark.sql(s"select count(*) from $dbtable where i < '2'").head().get(0)
    val actual = spark.sql(s"select count(*) from $dbtable").head().get(0)
    assert(expected == actual)
  }

  test("Delete non-cluster index table with int pk") {
    jdbcUpdate(
      s"create table $dbtable(i int, s int,PRIMARY KEY (i)/*T![clustered_index] NONCLUSTERED */,unique key (s))")
    jdbcUpdate(s"insert into $dbtable values(0,0),(1,1),(2,2),(3,3)")

    spark.sql(s"delete from $dbtable where i >= 2")

    val expected = spark.sql(s"select count(*) from $dbtable where i < 2").head().get(0)
    val actual = spark.sql(s"select count(*) from $dbtable").head().get(0)
    assert(expected == actual)
  }

  test("Delete non-cluster index table with varchar pk") {
    jdbcUpdate(
      s"create table $dbtable(i varchar(64), s int,PRIMARY KEY (i)/*T![clustered_index] NONCLUSTERED */)")
    jdbcUpdate(s"insert into $dbtable values('0',0),('1',1),('2',2),('3',3)")

    spark.sql(s"delete from $dbtable where i >= '2'")

    val expected = spark.sql(s"select count(*) from $dbtable where i < '2'").head().get(0)
    val actual = spark.sql(s"select count(*) from $dbtable").head().get(0)
    assert(expected == actual)
  }

  test("Delete table without pk") {
    jdbcUpdate(s"create table $dbtable(i int, s int ,unique key (s))")
    jdbcUpdate(s"insert into $dbtable values(0,0),(1,1),(2,2),(3,3)")

    spark.sql(s"delete from $dbtable where i >= 2")

    val expected = spark.sql(s"select count(*) from $dbtable where i < 2").head().get(0)
    val actual = spark.sql(s"select count(*) from $dbtable").head().get(0)
    assert(expected == actual)
  }

  test("Select after delete should not contains _tidb_rowid if it does not before") {
    jdbcUpdate(
      s"create table $dbtable(i varchar(64), s int,PRIMARY KEY (i)/*T![clustered_index] NONCLUSTERED */,unique key (s))")
    jdbcUpdate(s"insert into $dbtable values('0',0),('1',1),('2',2),('3',3)")

    val df1 = spark.sql(s"select * from $dbtable")
    spark.sql(s"delete from $dbtable where i >= '2'")
    val df2 = spark.sql(s"select * from $dbtable")

    assert(
      df1.schema.fieldNames
        .contains("_tidb_rowid") || (!df2.schema.fieldNames.contains("_tidb_rowid")))
  }
}
