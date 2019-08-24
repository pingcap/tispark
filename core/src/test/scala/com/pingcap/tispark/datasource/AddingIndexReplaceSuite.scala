package com.pingcap.tispark.datasource

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class AddingIndexReplaceSuite extends BaseDataSourceTest {
  private val row1 = Row(1, 1, 1, "Hello")
  private val row2 = Row(2, 2, 2, "TiDB")
  private val row3 = Row(3, 3, 3, "Spark")
  private val row4 = Row(4, 4, 4, "abde")
  private val row5 = Row(5, 5, 5, "Duplicate")

  private val conflcitWithOneIndex = Row(6, 4, 6, "abced")
  private val conflcitWithTwoIndices = Row(6, 5, 1, "adedefedede")

  private val schema = StructType(
    List(
      StructField("pk", IntegerType),
      StructField("c1", IntegerType),
      StructField("c2", IntegerType),
      StructField("s", StringType)
    )
  )

  test("test unique index replace with primary key is handle and index") {
    val table = "pk_is_handle_and_idx"
    dropTable(table)
    createTable(
      "create table `%s`.`%s`(pk int, c1 int, c2 int, s varchar(128), primary key(pk), unique index(c1), unique index(c2), index(s))",
      table
    )
    jdbcUpdate(
      "insert into `%s`.`%s` values(1, 1, 1, 'Hello')",
      table
    )
    // insert row2 row3
    tidbWriteWithTable(List(row2, row3, row4), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row3, row4), "c1", tableName = table)

    val options = Some(Map("replace" -> "true"))

    tidbWriteWithTable(List(row5, row5, conflcitWithOneIndex), schema, table, options)
    testTiDBSelectWithTable(
      Seq(row1, row2, row3, conflcitWithOneIndex, row5),
      "c1",
      tableName = table
    )

    tidbWriteWithTable(List(conflcitWithTwoIndices), schema, table, options)
    testTiDBSelectWithTable(Seq(row2, row3, conflcitWithTwoIndices), "c1", tableName = table)
  }

  test("test same key in one rdd") {
    val table = "same_key_in_one_rdd"
    dropTable(table)
    createTable(
      "create table `%s`.`%s`(pk int, c1 int, c2 int, s varchar(128), primary key(pk))",
      table
    )
    jdbcUpdate(
      "insert into `%s`.`%s` values(1, 1, 1, 'Hello')",
      table
    )

    // insert row2 row3
    tidbWriteWithTable(List(row2, row3, row4), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row3, row4), "c1", tableName = table)

    val options = Some(Map("replace" -> "true"))
    tidbWriteWithTable(List(row2), schema, table, options)
    testTiDBSelectWithTable(Seq(row1, row2, row3, row4), "c1", tableName = table)
  }

  test("test unique index replace with primary key is handle") {
    val table = "replace_pk_is_handle"
    dropTable(table)
    createTable(
      "create table `%s`.`%s`(pk int, c1 int, c2 int, s varchar(128), primary key(pk), unique index(c1), unique index(c2))",
      table
    )
    jdbcUpdate(
      "insert into `%s`.`%s` values(1, 1, 1, 'Hello')",
      table
    )
    // insert row2 row3
    tidbWriteWithTable(List(row2, row3, row4), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row3, row4), "c1", tableName = table)

    val options = Some(Map("replace" -> "true"))

    tidbWriteWithTable(List(row5, row5, conflcitWithOneIndex), schema, table, options)
    testTiDBSelectWithTable(
      Seq(row1, row2, row3, conflcitWithOneIndex, row5),
      "c1",
      tableName = table
    )

    tidbWriteWithTable(List(conflcitWithTwoIndices), schema, table, options)
    testTiDBSelectWithTable(Seq(row2, row3, conflcitWithTwoIndices), "c1", tableName = table)
  }

  test("test unique index replace without primary key") {
    val table = "no_pk"
    dropTable(table)
    createTable(
      "create table `%s`.`%s`(pk int, c1 int, c2 int, s varchar(128), unique index(c1), unique index(c2))",
      table
    )
    jdbcUpdate(
      "insert into `%s`.`%s` values(1, 1, 1, 'Hello')",
      table
    )
    // insert row2 row3
    tidbWriteWithTable(List(row2, row4, row5), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row4, row5), "c1", tableName = table)

    val options = Some(Map("replace" -> "true"))
    tidbWriteWithTable(
      List(row3, row3, conflcitWithOneIndex, conflcitWithTwoIndices),
      schema,
      table,
      options
    )
    testTiDBSelectWithTable(
      Seq(row2, row3, conflcitWithOneIndex, conflcitWithTwoIndices),
      "c1",
      tableName = table
    )
  }

  test("test pk is handle") {
    val table = "pk_is_handle"
    dropTable(table)
    createTable(
      "create table `%s`.`%s`(pk int, c1 int, c2 int, s varchar(128), primary key(pk))",
      table
    )
    jdbcUpdate(
      "insert into `%s`.`%s` values(1, 1, 1, 'Hello')",
      table
    )
    // insert row2 row3
    tidbWriteWithTable(List(row2, row3, row4), schema, table)
    testTiDBSelectWithTable(Seq(row1, row2, row3, row4), "c1", tableName = table)
  }
}
