package com.pingcap.tispark.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{BaseTiSparkTest, Row}
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

import java.sql.{Date, ResultSet}

class PartitionWriteSuite extends BaseTiSparkTest {

  val table: String = "test_partition_write"
  val database: String = "tispark_test"

  override def beforeEach(): Unit = {
    super.beforeEach()
    tidbStmt.execute(s"drop table if exists `$database`.`$table`")
  }

  test("hash partition append and delete test") {
    tidbStmt.execute(s"create table `$database`.`$table` (id int) partition by hash(id) PARTITIONS 4")

    val data: RDD[Row] = sc.makeRDD(List(Row(5), Row(35), Row(25), Row(15)))
    val schema: StructType =
      StructType(List(StructField("id", IntegerType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "false")
      .mode("append")
      .save()
    tidbStmt.execute(s"insert into `$database`.`$table` values (6), (7), (28), (29)")

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(Row(5), Row(6), Row(7), Row(15), Row(25), Row(35), Row(28), Row(29))
    checkJDBCResult(insertResultJDBC, Array(Array(5), Array(6), Array(7), Array(15), Array(25), Array(35), Array(28), Array(29)))

    spark.sql(s"delete from `tidb_catalog`.`$database`.`$table` where id < 16")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark.collect() should contain theSameElementsAs Array(Row(25), Row(35), Row(28), Row(29))
    checkJDBCResult(deleteResultJDBC, Array(Array(25), Array(35), Array(28), Array(29)))
  }

  test("hash partition replace and delete test") {
    tidbStmt.execute(s"create table `$database`.`$table` (id bigint primary key , name varchar(16)) partition by hash(id) PARTITIONS 4")

    tidbStmt.execute(s"insert into `$database`.`$table` values (5, 'Apple'), (25, 'Honey'), (29, 'Mike')")
    val data: RDD[Row] = sc.makeRDD(List(Row(5L, "Luo"), Row(25L, "John"), Row(15L, "Jack")))
    val schema: StructType =
      StructType(List(StructField("id", LongType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "true")
      .mode("append")
      .save()

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(Row(5L, "Luo"), Row(25L, "John"), Row(15L, "Jack"), Row(29, "Mike"))
    checkJDBCResult(insertResultJDBC, Array(Array(5L, "Luo"), Array(25L, "John"), Array(15L, "Jack"), Array(29, "Mike")))

    spark.sql(s"delete from `tidb_catalog`.`$database`.`$table` where id < 16 or name = 'Mike'")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark.collect() should contain theSameElementsAs Array(Row(25L, "John"))
    checkJDBCResult(deleteResultJDBC, Array(Array(25L, "John")))
  }

  test("hash YEAR() partition replace and delete test") {
    tidbStmt.execute(s"create table `$database`.`$table` (birthday date primary key , name varchar(16)) partition by hash(YEAR(birthday)) PARTITIONS 4")

    tidbStmt.execute(s"insert into `$database`.`$table` values ('1995-06-15', 'Apple'), ('1995-08-08', 'Honey'), ('1999-06-04', 'Mike')")
    val data: RDD[Row] = sc.makeRDD(List(Row(Date.valueOf("1995-06-15"), "Luo"), Row(Date.valueOf("1995-08-08"), "John"), Row(Date.valueOf("1993-08-22"), "Jack")))
    val schema: StructType =
      StructType(List(StructField("birthday", DateType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "true")
      .mode("append")
      .save()

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(Row(Date.valueOf("1995-06-15"), "Luo"), Row(Date.valueOf("1995-08-08"), "John"), Row(Date.valueOf("1993-08-22"), "Jack"), Row(Date.valueOf("1999-06-04"), "Mike"))
    checkJDBCResult(insertResultJDBC, Array(Array(Date.valueOf("1995-06-15"), "Luo"), Array(Date.valueOf("1995-08-08"), "John"), Array(Date.valueOf("1993-08-22"), "Jack"), Array(Date.valueOf("1999-06-04"), "Mike")))

    spark.sql(s"delete from `tidb_catalog`.`$database`.`$table` where birthday <= '1995-06-15' or name = 'Mike'")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark.collect() should contain theSameElementsAs Array(Row(Date.valueOf("1995-08-08"), "John"))
    checkJDBCResult(deleteResultJDBC, Array(Array(Date.valueOf("1995-08-08"), "John")))
  }

  test("hash range column replace and delete test") {
    tidbStmt.execute(s"create table `$database`.`$table` (id bigint, name varchar(16) unique key) partition by range columns(name) (" +
      s"partition p0 values less than ('BBBBBB')," +
      s"partition p1 values less than ('HHHHHH')," +
      s"partition p2 values less than MAXVALUE)")

    tidbStmt.execute(s"insert into `$database`.`$table` values (5, 'Apple'), (25, 'John'), (29, 'Mike')")
    val data: RDD[Row] = sc.makeRDD(List(Row(57L, "Apple"), Row(65L, "John"), Row(15L, "Jack")))
    val schema: StructType =
      StructType(List(StructField("id", LongType), StructField("name", StringType)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("replace", "true")
      .mode("append")
      .save()

    val insertResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val insertResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    insertResultSpark.collect() should contain theSameElementsAs Array(Row(57L, "Apple"), Row(65L, "John"), Row(15L, "Jack"), Row(29, "Mike"))
    checkJDBCResult(insertResultJDBC, Array(Array(57L, "Apple"), Array(65L, "John"), Array(15L, "Jack"), Array(29, "Mike")))

    spark.sql(s"delete from `tidb_catalog`.`$database`.`$table` where name < 'John' or name = 'Mike'")

    val deleteResultJDBC = tidbStmt.executeQuery(s"select * from `$database`.`$table`")
    val deleteResultSpark = spark.sql(s"select * from `tidb_catalog`.`$database`.`$table`")
    deleteResultSpark.collect() should contain theSameElementsAs Array(Row(65L, "John"))
    checkJDBCResult(deleteResultJDBC, Array(Array(65L, "John")))
  }


  def checkJDBCResult(resultJDBC: ResultSet, rows: Array[Array[_]]): Unit = {
    val rsMetaData = resultJDBC.getMetaData
    var sqlData: Seq[Seq[AnyRef]] = Seq()
    while (resultJDBC.next()) {
      var row: Seq[AnyRef] = Seq()
      for (i <- 1 to rsMetaData.getColumnCount) {
        row = row :+ resultJDBC.getObject(i)
      }
      sqlData = sqlData :+ row
    }
    sqlData should contain theSameElementsAs rows
  }
}


