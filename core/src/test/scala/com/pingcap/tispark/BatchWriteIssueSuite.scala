package com.pingcap.tispark

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class BatchWriteIssueSuite extends BaseDataSourceTest("test_batchwrite_issue") {
  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("Combine unique index with null value test") {
    doTestNullValues(
      s"create table $dbtable(a int, b varchar(64), CONSTRAINT ab UNIQUE (a, b))"
    )
  }

  test("Combine primary key with null value test") {
    doTestNullValues(
      s"create table $dbtable(a int, b varchar(64), PRIMARY KEY (a, b))"
    )
  }

  test("PK is handler with null value test") {
    doTestNullValues(
      s"create table $dbtable(a int, b varchar(64), PRIMARY KEY (a))"
    )
  }

  private def doTestNullValues(createTableSQL: String): Unit = {
    if (!supportBatchWrite) {
      cancel
    }
    val schema = StructType(
      List(
        StructField("a", IntegerType),
        StructField("b", StringType),
        StructField("c", StringType)
      )
    )

    val options = Some(Map("replace" -> "true"))

    dropTable()
    jdbcUpdate(createTableSQL)
    jdbcUpdate(s"alter table $dbtable add column to_delete int")
    jdbcUpdate(s"alter table $dbtable add column c varchar(64) default 'c33'")
    jdbcUpdate(s"alter table $dbtable drop column to_delete")
    jdbcUpdate(s"""
                  |insert into $dbtable values(11, 'c12', null);
                  |insert into $dbtable values(21, 'c22', null);
                  |insert into $dbtable (a, b) values(31, 'c32');
                  |insert into $dbtable values(41, 'c42', 'c43');
                  |
      """.stripMargin)

    assert(queryTiDBViaJDBC(s"select c from $dbtable where a=11").head.head == null)
    assert(queryTiDBViaJDBC(s"select c from $dbtable where a=21").head.head == null)
    assert(queryTiDBViaJDBC(s"select c from $dbtable where a=31").head.head.toString.equals("c33"))
    assert(queryTiDBViaJDBC(s"select c from $dbtable where a=41").head.head.toString.equals("c43"))

    {
      val row1 = Row(11, "c12", "c13")
      val row3 = Row(31, "c32", null)

      tidbWrite(List(row1, row3), schema, options)

      assert(
        queryTiDBViaJDBC(s"select c from $dbtable where a=11").head.head.toString.equals("c13")
      )
      assert(queryTiDBViaJDBC(s"select c from $dbtable where a=21").head.head == null)
      assert(queryTiDBViaJDBC(s"select c from $dbtable where a=31").head.head == null)
      assert(
        queryTiDBViaJDBC(s"select c from $dbtable where a=41").head.head.toString.equals("c43")
      )
    }

    {
      val row1 = Row(11, "c12", "c213")
      val row3 = Row(31, "c32", "tt")
      tidbWrite(List(row1, row3), schema, options)
      assert(
        queryTiDBViaJDBC(s"select c from $dbtable where a=11").head.head.toString.equals("c213")
      )
      assert(queryTiDBViaJDBC(s"select c from $dbtable where a=21").head.head == null)
      assert(queryTiDBViaJDBC(s"select c from $dbtable where a=31").head.head.toString.equals("tt"))
      assert(
        queryTiDBViaJDBC(s"select c from $dbtable where a=41").head.head.toString.equals("c43")
      )
    }
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
