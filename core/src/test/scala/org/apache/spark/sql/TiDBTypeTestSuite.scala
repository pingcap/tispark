package org.apache.spark.sql

import com.pingcap.tikv.types.Converter

class TiDBTypeTestSuite extends BaseTiSparkSuite {
  test("adding time type index test") {
    tidbStmt.execute("drop table if exists t_t")
    tidbStmt.execute("CREATE TABLE `t_t` (`t` time(3), index `idx_t`(t))")
    // NOTE: jdbc only allows time in day range whereas mysql time has much
    // larger range.
    tidbStmt.execute("INSERT INTO t_t (t) VALUES('18:59:59'),('17:59:59'),('12:59:59')")
    refreshConnections()
    val df = spark.sql("select * from t_t")
    val data = dfData(df, df.schema.fields)
    assert(data(0)(0).asInstanceOf[Long].equals(Converter.convertStrToDuration("18:59:59")))
    assert(data(1)(0).asInstanceOf[Long].equals(Converter.convertStrToDuration("17:59:59")))
    assert(data(2)(0).asInstanceOf[Long].equals(Converter.convertStrToDuration("12:59:59")))

    val where = spark.sql("select * from t_t where t = 46799000000000")
    val wheredata = dfData(where, where.schema.fields)
    assert(wheredata(0)(0).asInstanceOf[Long].equals(Converter.convertStrToDuration("12:59:59")))
  }

  test("adding time type") {
    tidbStmt.execute("drop table if exists t_t")
    tidbStmt.execute("CREATE TABLE `t_t` (`t` time(3))")
    // NOTE: jdbc only allows time in day range whereas mysql time has much
    // larger range.
    tidbStmt.execute("INSERT INTO t_t (t) VALUES('18:59:59'),('17:59:59'),('12:59:59')")
    refreshConnections()
    val df = spark.sql("select * from t_t")
    val data = dfData(df, df.schema.fields)
    assert(data(0)(0).asInstanceOf[Long].equals(Converter.convertStrToDuration("18:59:59")))
    assert(data(1)(0).asInstanceOf[Long].equals(Converter.convertStrToDuration("17:59:59")))
    assert(data(2)(0).asInstanceOf[Long].equals(Converter.convertStrToDuration("12:59:59")))

    val where = spark.sql("select * from t_t where t = 46799000000000")
    val wheredata = dfData(where, where.schema.fields)
    assert(wheredata(0)(0).asInstanceOf[Long].equals(Converter.convertStrToDuration("12:59:59")))
  }

  test("adding year type") {
    tidbStmt.execute("drop table if exists y_t")
    tidbStmt.execute("CREATE TABLE `y_t` (`y4` year(4))")
    tidbStmt.execute("INSERT INTO y_t (y4) VALUES(1912),(2012),(2112)")
    refreshConnections()
    judge("select * from y_t")
    judge("select * from y_t where y4 = 2112")
  }

  test("adding set and enum") {
    tidbStmt.execute("drop table if exists set_t")
    tidbStmt.execute("drop table if exists enum_t")
    tidbStmt.execute(
      " CREATE TABLE `set_t` (\n  `col` " +
        "set('1','2','3','4', '5', '6','7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29','30','31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49','50','51', '52', '53', '54', '55', '56','57', '58', '59', '60', '61', '62', '63', '64')\n)"
    )
    tidbStmt.execute("INSERT INTO set_t(col) VALUES('1')")
    tidbStmt.execute("INSERT INTO set_t(col) VALUES('1,3')")
    tidbStmt.execute("INSERT INTO set_t(col) VALUES('1,32')")
    tidbStmt.execute("INSERT INTO set_t(col) VALUES('1,63')")
    tidbStmt.execute("INSERT INTO set_t(col) VALUES('1,64')")
    tidbStmt.execute(
      "CREATE TABLE `enum_t` (" +
        "`priority` set('Low','Medium','High') NOT NULL)"
    )
    tidbStmt.execute("INSERT INTO enum_t(priority) VALUES('High')")
    tidbStmt.execute("INSERT INTO enum_t(priority) VALUES('Medium')")
    tidbStmt.execute("INSERT INTO enum_t(priority) VALUES('Low')")
    refreshConnections()
    judge("select * from set_t")
    judge("select * from set_t where col = '1'")
    judge("select * from enum_t")
    judge("select * from enum_t where priority = 'High'")
  }

  test("adding json support") {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("create table t(json_doc json)")
    tidbStmt.execute(
      """insert into t values  ('null'),
          ('true'),
          ('false'),
          ('0'),
          ('1'),
          ('-1'),
          ('2147483647'),
          ('-2147483648'),
          ('9223372036854775807'),
          ('-9223372036854775808'),
          ('0.5'),
          ('-0.5'),
          ('""'),
          ('"a"'),
          ('"\\t"'),
          ('"\\n"'),
          ('"\\""'),
          ('"\\u0001"'),
          ('[]'),
          ('"中文"'),
          (JSON_ARRAY(null, false, true, 0, 0.5, "hello", JSON_ARRAY("nested_array"), JSON_OBJECT("nested", "object"))),
          (JSON_OBJECT("a", null, "b", true, "c", false, "d", 0, "e", 0.5, "f", "hello", "nested_array", JSON_ARRAY(1, 2, 3), "nested_object", JSON_OBJECT("hello", 1)))"""
    )
    refreshConnections()

    runTest(
      "select json_doc from t",
      skipJDBC = true,
      rTiDB = List(
        List("null"),
        List(true),
        List(false),
        List(0),
        List(1),
        List(-1),
        List(2147483647),
        List(-2147483648),
        List(9223372036854775807L),
        List(-9223372036854775808L),
        List(0.5),
        List(-0.5),
        List("\"\""),
        List("\"a\""),
        List("\"\\t\""),
        List("\"\\n\""),
        List("\"\\\"\""),
        List("\"\\u0001\""),
        List("[]"),
        List("\"中文\""),
        List("[null,false,true,0,0.5,\"hello\",[\"nested_array\"],{\"nested\":\"object\"}]"),
        List(
          "{\"a\":null,\"b\":true,\"c\":false,\"d\":0,\"e\":0.5,\"f\":\"hello\",\"nested_array\":[1,2,3],\"nested_object\":{\"hello\":1}}"
        )
      )
    )
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists set_t")
      tidbStmt.execute("drop table if exists enum_t")
      tidbStmt.execute("drop table if exists t_t")
      tidbStmt.execute("drop table if exists y_t")
      tidbStmt.execute("drop table if exists t")
    } finally {
      super.afterAll()
    }
}
