package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

// TODO: once exception message is stable, we need also check the exception's message.
class AddingIndexInsertSuite extends BaseDataSourceTest {
  private val row1 = Row(1, 1, "Hello")
  private val row2 = Row(2, 2, "TiDB")
  private val row3 = Row(3, 3, "Spark")
  private val row4 = Row(4, 4, "abde")
  private val row5 = Row(5, 5, "Duplicate")

  private val schema = StructType(
    List(
      StructField("pk", IntegerType),
      StructField("i", IntegerType),
      StructField("s", StringType)
    )
  )

  test("test column type can be truncated") {
    val table = "test_col_tp"
    dropTable(table)
    jdbcUpdate(
      s"create table `$database`.`$table`(pk int, i int, s varchar(128), unique index(s(2)))"
    )
    jdbcUpdate(
      s"insert into `$database`.`$table` values(1, 1, 'Hello')"
    )
    // insert row2 row3
    tidbWriteWithTable(List(row2, row3), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row3), tableName = table)

    intercept[TiBatchWriteException] {
      // insert row2 row4
      tidbWriteWithTable(List(row2, row4), schema, table)
    }
  }

  test("no pk, no unique index case") {
    val table = "no_pk_no_idx"
    dropTable(table)
    jdbcUpdate(
      s"create table `$database`.`$table`(pk int, i int, s varchar(128), index(i))"
    )
    jdbcUpdate(
      s"insert into `$database`.`$table` values(1, 1, 'Hello')"
    )
    // insert row2 row3
    tidbWriteWithTable(List(row2, row3), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row3), tableName = table)

    tidbWriteWithTable(List(row2, row4), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row2, row3, row4), tableName = table)

    tidbWriteWithTable(List(row4, row4), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row2, row3, row4, row4, row4), tableName = table)
  }

  test("pk is not handle adding unique index") {
    val table = "pk_is_not_handle"
    dropTable(table)
    jdbcUpdate(
      s"create table `$database`.`$table`(pk int, i int, s varchar(128), unique index(i), primary key(s))"
    )
    jdbcUpdate(
      s"insert into `$database`.`$table` values(1, 1, 'Hello')"
    )
    // insert row2 row3
    tidbWriteWithTable(List(row2, row3), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row3), tableName = table)

    intercept[TiBatchWriteException] {
      // insert row2 row4
      tidbWriteWithTable(List(row2, row4), schema, table)
    }
  }

  test("pk is handle adding unique index") {
    val table = "pk_is_handle"
    dropTable(table)
    jdbcUpdate(
      s"create table `$database`.`$table`(pk int, i int, s varchar(128), unique index(i), primary key(pk))"
    )
    jdbcUpdate(
      s"insert into `$database`.`$table` values(1, 1, 'Hello')"
    )
    // insert row2 row3
    tidbWriteWithTable(List(row2, row3), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row3), tableName = table)

    intercept[TiBatchWriteException] {
      // insert row2 row4
      tidbWriteWithTable(List(row2, row4), schema, table)
    }
  }

  test("Test no pk adding unique index") {
    val table = "no_pk"
    dropTable(table)
    jdbcUpdate(s"create table `$database`.`$table`(pk int, i int, s varchar(128), unique index(i))")
    jdbcUpdate(
      s"insert into `$database`.`$table` values(1, 1, 'Hello')"
    )

    // insert row2 row3
    tidbWriteWithTable(List(row2, row3), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row3), tableName = table)

    intercept[TiBatchWriteException] {
      // insert row2 row4
      tidbWriteWithTable(List(row2, row4), schema, table)
    }
  }
}
