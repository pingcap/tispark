package com.pingcap.tispark.datatype

import java.sql.Timestamp

import com.pingcap.tikv.types.TypeSystem
import com.pingcap.tispark.datasource.BaseDataSourceTest

class CompareV0AndV1Suite extends BaseDataSourceTest("test_data_type_compare_v0_v1", "test") {

  test("Test compare BIT(1) Type") {
    val createTableQuery = s"create table $dbtable(i INT, c1 BIT(1))"
    val insertQuery = s"insert into $dbtable values(1, b'1'),(2, b'0')"

    {
      val answerV0 = 1L :: 0L :: Nil
      val answerV1 = true :: false :: Nil
      doTestSelect(createTableQuery, insertQuery, answerV0, answerV1)
    }

    {
      val answerV0 = 0L :: Nil
      val answerV1 = false :: Nil
      val where = "where c1 = 0"
      doTestSelect(createTableQuery, insertQuery, answerV0, answerV1, where = where)
    }

    {
      val answerV0 = 1L :: Nil
      val answerV1 = true :: Nil
      val where = "where c1 = true"
      doTestSelect(createTableQuery, insertQuery, answerV0, answerV1, where = where)
    }

  }

  test("Test compare BIT(4) Type") {
    val createTableQuery = s"create table $dbtable(i INT, c1 BIT(4))"
    val insertQuery =
      s"insert into $dbtable values(1, b'0000'),(2, b'0101'),(3, b'1010'),(4, b'1111')"
    val answerV0 = 0L :: 5L :: 10L :: 15L :: Nil
    val answerV1 = toBytes(0) :: toBytes(5) :: toBytes(10) :: toBytes(15) :: Nil
    val skipSparkJDBC = true // cause spark jdbc return BooleanType

    doTestSelect(createTableQuery, insertQuery, answerV0, answerV1, skipSparkJDBC = skipSparkJDBC)
  }

  test("Test compare BIT(8) Type") {
    val createTableQuery = s"create table $dbtable(i INT, c1 BIT(8))"
    val insertQuery =
      s"insert into $dbtable values(1, b'00000000'),(2, b'01010101'),(3, b'10101010'),(4, b'11111111')"
    val answerV0 = 0L :: 85L :: 170L :: 255L :: Nil
    val answerV1 = toBytes(0) :: toBytes(85) :: toBytes(170) :: toBytes(255) :: Nil
    val skipSparkJDBC = true // cause spark jdbc return BooleanType

    doTestSelect(createTableQuery, insertQuery, answerV0, answerV1, skipSparkJDBC = skipSparkJDBC)
  }

  test("Test compare BIT(10) Type") {
    val createTableQuery = s"create table $dbtable(i INT, c1 BIT(10))"
    val insertQuery =
      s"insert into $dbtable values(1, b'0000000000'),(2, b'0101010101'),(3, b'1010101010'),(4, b'1111111111')"
    val answerV0 = 0L :: 341L :: 682L :: 1023L :: Nil
    val answerV1 = toBytes(0, 0) :: toBytes(1, 85) :: toBytes(2, 170) :: toBytes(3, 255) :: Nil
    val skipSparkJDBC = true // cause spark jdbc return BooleanType

    doTestSelect(createTableQuery, insertQuery, answerV0, answerV1, skipSparkJDBC = skipSparkJDBC)
  }

  test("Test compare BIT(64) Type") {
    val v1 = "0000000000000000000000000000000000000000000000000000000000000000"
    val v2 = "0101010101010101010101010101010101010101010101010101010101010101"
    val v3 = "1111111111111111111111111111111111111111111111111111111111111111"

    val b1 = toBytes(0, 0, 0, 0, 0, 0, 0, 0)
    val b2 = toBytes(85, 85, 85, 85, 85, 85, 85, 85)
    val b3 = toBytes(255, 255, 255, 255, 255, 255, 255, 255)

    val createTableQuery = s"create table $dbtable(i INT, c1 BIT(64))"
    val insertQuery = s"insert into $dbtable values(1, b'$v1'),(2, b'$v2'),(3, b'$v3')"
    val answerV0 = 0L :: 6148914691236517205L :: -1L :: Nil
    val answerV1 = b1 :: b2 :: b3 :: Nil
    val skipSparkJDBC = true // cause spark jdbc return BooleanType

    doTestSelect(createTableQuery, insertQuery, answerV0, answerV1, skipSparkJDBC = skipSparkJDBC)
  }

  test("Test compare TINYINT Type") {
    val createTableQuery = s"create table $dbtable(i INT, c1 TINYINT)"
    val insertQuery = s"insert into $dbtable values(1, 1),(2, 2),(3, 127), (4, -128)"
    val answerV0 = 1L :: 2L :: 127L :: -128L :: Nil
    val answerV1 = 1 :: 2 :: 127 :: -128 :: Nil

    doTestSelect(createTableQuery, insertQuery, answerV0, answerV1)
  }

  test("Test compare BOOLEAN Type") {
    val createTableQuery = s"create table $dbtable(i INT, c1 BOOLEAN)"
    val insertQuery = s"insert into $dbtable values(1, true),(2, false)"
    val answerV0 = 1L :: 0L :: Nil
    val answerV1 = true :: false :: Nil

    doTestSelect(createTableQuery, insertQuery, answerV0, answerV1)
  }

  test("Test compare SMALLINT Type") {
    val createTableQuery = s"create table $dbtable(i INT, c1 SMALLINT)"
    val insertQuery = s"insert into $dbtable values(1, 1),(2, 2),(3, 32767), (4, -32768)"
    val answerV0 = 1L :: 2L :: 32767L :: -32768L :: Nil
    val answerV1 = 1 :: 2 :: 32767 :: -32768 :: Nil

    doTestSelect(createTableQuery, insertQuery, answerV0, answerV1)
  }

  test("Test compare MEDIUMINT Type") {
    val createTableQuery = s"create table $dbtable(i INT, c1 MEDIUMINT)"
    val insertQuery = s"insert into $dbtable values(1, 1),(2, 2),(3, 8388607), (4, -8388608)"
    val answerV0 = 1L :: 2L :: 8388607L :: -8388608L :: Nil
    val answerV1 = 1 :: 2 :: 8388607 :: -8388608 :: Nil

    doTestSelect(createTableQuery, insertQuery, answerV0, answerV1)
  }

  test("Test compare INT Type") {
    val createTableQuery = s"create table $dbtable(i INT, c1 INT)"
    val insertQuery = s"insert into $dbtable values(1, 1),(2, 2),(3, 2147483647), (4, -2147483648)"
    val answerV0 = 1L :: 2L :: 2147483647L :: -2147483648L :: Nil
    val answerV1 = 1 :: 2 :: 2147483647 :: -2147483648 :: Nil

    doTestSelect(createTableQuery, insertQuery, answerV0, answerV1)
  }

  test("Test compare TIME Type") {
    val createTableQuery = s"create table $dbtable(i INT, c1 TIME)"
    val insertQuery =
      s"insert into $dbtable values(1, '00:00:00'),(2, '11:12:13'),(3, '23:59:59'), (4, '24:59:59')"
    val answerV0 = 0L :: 40333000000000L :: 86399000000000L :: 89999000000000L :: Nil
    val answerV1 = Timestamp.valueOf("1970-01-01 00:00:00") :: Timestamp.valueOf(
      "1970-01-01 11:12:13"
    ) :: Timestamp.valueOf("1970-01-01 23:59:59") :: Timestamp.valueOf("1970-01-02 00:59:59") :: Nil

    doTestSelect(createTableQuery, insertQuery, answerV0, answerV1)
  }

  test("Test compare YEAR Type") {
    val createTableQuery = s"create table $dbtable(i INT, c1 YEAR)"
    val insertQuery = s"insert into $dbtable values(1, '1999'),(2, '2000')"
    val answerV0 = 1999L :: 2000L :: Nil
    val answerV1 = java.sql.Date.valueOf("1999-01-01") :: java.sql.Date.valueOf("2000-01-01") :: Nil

    doTestSelect(createTableQuery, insertQuery, answerV0, answerV1)
  }

  private def doTestSelect(createTableQuery: String,
                           insertQuery: String,
                           answerV0: List[Any],
                           answerV1: List[Any],
                           where: String = "",
                           skipJDBC: Boolean = false,
                           skipSparkJDBC: Boolean = false): Unit = {
    // create table
    dropTable()
    jdbcUpdate(createTableQuery)

    // insert data
    jdbcUpdate(insertQuery)

    // select using jdbc and assert
    val (_, resJDBC) = queryViaJDBC(s"select * from `$database`.`$table` $where")
    if (!skipJDBC) {
      for (i <- resJDBC.indices) {
        assert(isEqual(answerV1(i), resJDBC(i)(1)))
      }
    }

    // select using spark jdbc and assert
    val dfSparkJDBC = queryViaSparkJDBC(s"(select * from `$database`.`$table` $where) as tmp")
    val resSparkJDBC = dfSparkJDBC.collect()
    if (!skipSparkJDBC) {
      for (i <- resSparkJDBC.indices) {
        assert(isEqual(answerV1(i), resSparkJDBC(i).get(1)))

      }
    }

    // select using spark tidb with TypeSystemVersion = 0 and assert
    TypeSystem.setVersion(0)
    val dfSparkTiDBV0 = queryViaSparkTiDB(s"select * from `$dbPrefix$database`.`$table` $where")
    val resSparkTiDBV0 = dfSparkTiDBV0.collect()
    for (i <- resSparkTiDBV0.indices) {
      assert(isEqual(answerV0(i), resSparkTiDBV0(i).get(1)))
      assert(!isEqual(answerV1(i), resSparkTiDBV0(i).get(1)))
    }

    // select using spark tidb with TypeSystemVersion = 1 and assert
    TypeSystem.setVersion(1)
    val dfSparkTiDBV1 = queryViaSparkTiDB(s"select * from `$dbPrefix$database`.`$table` $where")
    val resSparkTiDBV1 = dfSparkTiDBV1.collect()
    for (i <- resSparkTiDBV1.indices) {
      assert(isEqual(answerV1(i), resSparkTiDBV1(i).get(1)))
      assert(!isEqual(answerV0(i), resSparkTiDBV1(i).get(1)))
    }
  }

  private def isEqual(lhs: Any, rhs: Any): Boolean = {
    lhs match {
      case l: Array[Byte] if rhs.isInstanceOf[Array[Byte]] =>
        val r = rhs.asInstanceOf[Array[Byte]]
        if (l.length != r.length) {
          false
        } else {
          for (pos <- l.indices) {
            if (l(pos) != r(pos)) {
              return false
            }
          }
          true
        }
      case l: java.sql.Timestamp if rhs.isInstanceOf[java.sql.Time] =>
        val r = rhs.asInstanceOf[java.sql.Time]
        r.equals(l)
      case _ =>
        lhs.equals(rhs)
    }
  }

  override def afterAll(): Unit = {
    TypeSystem.resetVersion()
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
  }
}
