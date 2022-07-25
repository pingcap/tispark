package com.pingcap.tispark.insert

import com.pingcap.tispark.datasource.BaseBatchWriteTest

class InsertSuite extends BaseBatchWriteTest("test_delete_compatibility") {
  private val source_dbtable = "tispark_test.source_table"
  override def dropTable(): Unit = {
    jdbcUpdate(s"drop table if exists $dbtable")
    jdbcUpdate(s"drop table if exists $source_dbtable")
  }

  test("insert basic test") {
    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")

    spark.sql(s"INSERT INTO $dbtable VALUES(0,'hello'),(1,'world')")

    spark.sql(s"SELECT * FROM $dbtable")

    val actual = spark.sql(s"SELECT count(*) FROM $dbtable").head().get(0)
    assert(2 == actual)
  }

  test("insert select test") {
    jdbcUpdate(
      s"create table $source_dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")

    spark.sql(s"INSERT INTO $source_dbtable VALUES(0,'hello'),(1,'world')")

    spark.sql(s"INSERT INTO $dbtable SELECT * FROM $source_dbtable")

    val actual = spark.sql(s"SELECT count(*) FROM $dbtable").head().get(0)
    assert(2 == actual)
  }

  test("insert select with condition test") {
    jdbcUpdate(
      s"create table $source_dbtable(i int, s varchar(255), k varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")

    spark.sql(s"INSERT INTO $source_dbtable VALUES(0,'hello','tidb'),(1,'world','test')")

    spark.sql(s"INSERT INTO $dbtable SELECT i, s FROM $source_dbtable WHERE i >= 1")

    val actual = spark.sql(s"SELECT count(*) FROM $dbtable").head().get(0)
    assert(1 == actual)
  }

  test("insert table test") {
    jdbcUpdate(
      s"create table $source_dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")

    spark.sql(s"INSERT INTO $source_dbtable VALUES(0,'hello'),(1,'world')")

    spark.sql(s"INSERT INTO $dbtable table $source_dbtable")

    val actual = spark.sql(s"SELECT count(*) FROM $dbtable").head().get(0)
    assert(2 == actual)
  }

  test("insert from test") {
    jdbcUpdate(
      s"create table $source_dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")

    spark.sql(s"INSERT INTO $source_dbtable VALUES(0,'hello'),(1,'world')")

    spark.sql(s"INSERT INTO $dbtable FROM $source_dbtable SELECT *")

    val actual = spark.sql(s"SELECT count(*) FROM $dbtable").head().get(0)
    assert(2 == actual)
  }

  test("insert from with condition test") {
    jdbcUpdate(
      s"create table $source_dbtable(i int, s varchar(255), k varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")

    spark.sql(s"INSERT INTO $source_dbtable VALUES(0,'hello','tidb'),(1,'world','test')")

    spark.sql(s"INSERT INTO $dbtable FROM $source_dbtable SELECT i,s WHERE i>=1")

    val actual = spark.sql(s"SELECT count(*) FROM $dbtable").head().get(0)
    assert(1 == actual)
  }
}
