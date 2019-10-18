package com.pingcap.tispark.datatype

import java.math.BigInteger

import com.pingcap.tikv.types.{MySQLType, TypeSystem}
import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.types

class TypeSystemSuite extends BaseDataSourceTest("test_data_type_system", "test") {

  case class Result(mysqlType: MySQLType,
                    jdbcType: String,
                    sparkTiDBTypeV0: types.DataType,
                    sparkTiDBTypeV1: types.DataType,
                    sparkJDBCType: types.DataType)

  private val results =
    //0 INT -> IntegerType:TypeLong
    Result(MySQLType.TypeLong, "INT", types.LongType, types.IntegerType, types.IntegerType) ::
      //1 BIT(1) -> BitType:TypeBit
      Result(MySQLType.TypeBit, "BIT", types.LongType, types.BooleanType, types.BooleanType) ::
      //2 BIT(8) -> BitType:TypeBit
      Result(MySQLType.TypeBit, "BIT", types.LongType, types.BinaryType, types.BooleanType) ::
      //3 TINYINT -> IntegerType:TypeTiny
      Result(MySQLType.TypeTiny, "TINYINT", types.LongType, types.IntegerType, types.IntegerType) ::
      //4 BOOLEAN -> IntegerType:TypeTiny
      Result(MySQLType.TypeTiny, "TINYINT", types.LongType, types.BooleanType, types.BooleanType) ::
      //5 SMALLINT -> IntegerType:TypeShort
      Result(MySQLType.TypeShort, "SMALLINT", types.LongType, types.IntegerType, types.IntegerType) ::
      //6 MEDIUMINT -> IntegerType:TypeInt24
      Result(MySQLType.TypeInt24, "MEDIUMINT", types.LongType, types.IntegerType, types.IntegerType) ::
      //7 INTEGER -> IntegerType:TypeLong
      Result(MySQLType.TypeLong, "INT", types.LongType, types.IntegerType, types.IntegerType) ::
      //8 BIGINT -> IntegerType:TypeLonglong
      Result(MySQLType.TypeLonglong, "BIGINT", types.LongType, types.LongType, types.LongType) ::
      //9 FLOAT -> RealType:TypeFloat
      Result(MySQLType.TypeFloat, "FLOAT", types.DoubleType, types.DoubleType, types.DoubleType) ::
      //10 DOUBLE -> RealType:TypeDouble
      Result(MySQLType.TypeDouble, "DOUBLE", types.DoubleType, types.DoubleType, types.DoubleType) ::
      //11 DECIMAL -> DecimalType:TypeNewDecimal
      Result(
      MySQLType.TypeNewDecimal,
      "DECIMAL",
      types.DecimalType(11, 0),
      types.DecimalType(11, 0),
      types.DecimalType(11, 0)
    ) ::
      //12 DATE -> DateType:TypeDate
      Result(MySQLType.TypeDate, "DATE", types.DateType, types.DateType, types.DateType) ::
      //13 DATETIME -> DateTimeType:TypeDatetime
      Result(
      MySQLType.TypeDatetime,
      "DATETIME",
      types.TimestampType,
      types.TimestampType,
      types.TimestampType
    ) ::
      //14 TIMESTAMP -> TimestampType:TypeTimestamp
      Result(
      MySQLType.TypeTimestamp,
      "TIMESTAMP",
      types.TimestampType,
      types.TimestampType,
      types.TimestampType
    ) ::
      //15 TIME -> TimeType:TypeDuration
      Result(
      MySQLType.TypeDuration,
      "TIME",
      types.LongType,
      types.TimestampType,
      types.TimestampType
    ) ::
      //16 YEAR -> IntegerType:TypeYear
      Result(MySQLType.TypeYear, "YEAR", types.LongType, types.DateType, types.DateType) ::
      //17 CHAR(64) -> StringType:TypeString
      Result(MySQLType.TypeString, "CHAR", types.StringType, types.StringType, types.StringType) ::
      //18 VARCHAR(64) -> StringType:TypeVarchar
      Result(MySQLType.TypeVarchar, "VARCHAR", types.StringType, types.StringType, types.StringType) ::
      //19 BINARY(64) -> BytesType:TypeString
      Result(MySQLType.TypeString, "BINARY", types.BinaryType, types.BinaryType, types.BinaryType) ::
      //20 VARBINARY(64) -> BytesType:TypeVarchar
      Result(
      MySQLType.TypeVarchar,
      "VARBINARY",
      types.BinaryType,
      types.BinaryType,
      types.BinaryType
    ) ::
      //21 TINYBLOB -> BytesType:TypeTinyBlob
      Result(
      MySQLType.TypeTinyBlob,
      "TINYBLOB",
      types.BinaryType,
      types.BinaryType,
      types.BinaryType
    ) ::
      //22 TINYTEXT -> StringType:TypeTinyBlob
      Result(
      MySQLType.TypeTinyBlob,
      "TINYBLOB",
      types.StringType,
      types.StringType,
      types.StringType
    ) ::
      //23 BLOB -> BytesType:TypeBlob
      Result(MySQLType.TypeBlob, "BLOB", types.BinaryType, types.BinaryType, types.BinaryType) ::
      //24 TEXT -> StringType:TypeBlob
      Result(MySQLType.TypeBlob, "VARCHAR", types.StringType, types.StringType, types.StringType) ::
      //25 MEDIUMBLOB -> BytesType:TypeMediumBlob
      Result(
      MySQLType.TypeMediumBlob,
      "MEDIUMBLOB",
      types.BinaryType,
      types.BinaryType,
      types.BinaryType
    ) ::
      //26 MEDIUMTEXT -> StringType:TypeMediumBlob
      Result(
      MySQLType.TypeMediumBlob,
      "MEDIUMBLOB",
      types.StringType,
      types.StringType,
      types.StringType
    ) ::
      //27 LONGBLOB -> BytesType:TypeLongBlob
      Result(
      MySQLType.TypeLongBlob,
      "LONGBLOB",
      types.BinaryType,
      types.BinaryType,
      types.BinaryType
    ) ::
      //28 LONGTEXT -> StringType:TypeLongBlob
      Result(
      MySQLType.TypeLongBlob,
      "LONGBLOB",
      types.StringType,
      types.StringType,
      types.StringType
    ) ::
      //29 ENUM('male', 'female', 'both', 'unknow')-> EnumType:TypeEnum
      Result(MySQLType.TypeEnum, "CHAR", types.StringType, types.StringType, types.StringType) ::
      //30 SET('a', 'b', 'c') -> SetType:TypeSet
      Result(MySQLType.TypeSet, "CHAR", types.StringType, types.StringType, types.StringType) ::
      //31 INT UNSIGNED
      Result(MySQLType.TypeLong, "INT UNSIGNED", types.LongType, types.LongType, types.LongType) ::
      //32 BIGINT UNSIGNED
      Result(
      MySQLType.TypeLonglong,
      "BIGINT UNSIGNED",
      types.DecimalType(20, 0),
      types.DecimalType(20, 0),
      types.DecimalType(20, 0)
    ) ::
      Nil

  test("Test Read different types with TypeSystemVersion=0") {
    parepareData()

    TypeSystem.setVersion(0)

    // assert Schema Type
    val tiTableInfo = getTableInfo(database, table)
    for (i <- 0 until tiTableInfo.getColumns.size()) {
      assert(tiTableInfo.getColumn(i).getType.getType == results(i).mysqlType)
    }

    // JDBC Data Type
    val (schemaJDBC, resJDBC) = queryViaJDBC(s"select * from `$database`.`$table`")
    for (i <- schemaJDBC.indices) {
      assert(schemaJDBC(i) == results(i).jdbcType)
    }

    // JDBC DataFrame Data Type
    val dfSparkJDBC = queryViaSparkJDBC(s"`$database`.`$table`")
    dfSparkJDBC.show(false)
    val schemaSparkJDBC = dfSparkJDBC.schema.toList
    for (i <- schemaSparkJDBC.indices) {
      assert(schemaSparkJDBC(i).dataType == results(i).sparkJDBCType)
    }

    // TiDB DataFrame Data Type
    val dfSparkTiDB = queryViaSparkTiDB(s"select * from `$dbPrefix$database`.`$table`")
    dfSparkTiDB.show(false)
    val schemaSparkTiDB = dfSparkTiDB.schema.toList
    for (i <- schemaSparkTiDB.indices) {
      assert(schemaSparkTiDB(i).dataType == results(i).sparkTiDBTypeV0)
    }
  }

  test("Test Read different types with TypeSystemVersion=1") {
    parepareData()

    TypeSystem.setVersion(1)

    // assert Schema Type
    val tiTableInfo = getTableInfo(database, table)
    for (i <- 0 until tiTableInfo.getColumns.size()) {
      assert(tiTableInfo.getColumn(i).getType.getType == results(i).mysqlType)
    }

    // JDBC Data Type
    val (schemaJDBC, resJDBC) = queryViaJDBC(s"select * from `$database`.`$table`")
    for (i <- schemaJDBC.indices) {
      assert(schemaJDBC(i) == results(i).jdbcType)
    }

    // JDBC DataFrame Data Type
    val dfSparkJDBC = queryViaSparkJDBC(s"`$database`.`$table`")
    dfSparkJDBC.show(false)
    val schemaSparkJDBC = dfSparkJDBC.schema.toList
    for (i <- schemaSparkJDBC.indices) {
      assert(schemaSparkJDBC(i).dataType == results(i).sparkJDBCType)
    }

    // TIDB DataFrame Data Type
    val dfSparkTiDB = queryViaSparkTiDB(s"select * from `$dbPrefix$database`.`$table`")
    dfSparkTiDB.show(false)
    val schemaSparkTiDB = dfSparkTiDB.schema.toList
    for (i <- schemaSparkTiDB.indices) {
      assert(schemaSparkTiDB(i).dataType == results(i).sparkTiDBTypeV1)
    }

    // JDBC DataFrame Data VS TIDB DataFrame Data
    val dataSparkTiDB = dfSparkTiDB.collect().head
    val dataSparkJDBC = dfSparkJDBC.collect().head
    val dataJDBC = resJDBC.head
    assert(dataSparkTiDB.size == dataSparkJDBC.size)
    for (i <- 0 until dataSparkTiDB.size) {
      // 2	true	[B@6f7a65e BIT(8) spark bug
      // 14	2019-01-01 18:11:11.0	2019-01-01 11:11:11.0 TIMESTAMP spark bug
      if (i != 2 && i != 14) {
        assert(isEqual(dataSparkJDBC.get(i), dataSparkTiDB.get(i)))
      }
    }

    // JDBC Data VS TIDB DataFrame Data
    assert(dataJDBC.size == dataSparkTiDB.size)
    for (i <- 0 until dataSparkTiDB.size) {
      assert(isEqual(dataJDBC(i), dataSparkTiDB.get(i)))
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
      case l: Double if rhs.isInstanceOf[Double] =>
        val r = rhs.asInstanceOf[Double]
        Math.abs(l - r) < 10e-6
      case l: Float if rhs.isInstanceOf[Double] =>
        val r = rhs.asInstanceOf[Double]
        Math.abs(l - r) < 10e-6
      case l: Double if rhs.isInstanceOf[Float] =>
        val r = rhs.asInstanceOf[Float]
        Math.abs(l - r) < 10e-6
      case l: java.math.BigDecimal if rhs.isInstanceOf[BigInteger] =>
        val r = rhs.asInstanceOf[BigInteger]
        l.toBigInteger.equals(r)
      case l: BigInteger if rhs.isInstanceOf[java.math.BigDecimal] =>
        val r = rhs.asInstanceOf[java.math.BigDecimal]
        l.equals(r.toBigInteger)
      case _ =>
        lhs.equals(rhs)
    }
  }

  private def parepareData(): Unit = {
    dropTable()
    jdbcUpdate(s"""
                  |create table $dbtable(
                  |i INT,
                  |c1 BIT(1),
                  |c2 BIT(8),
                  |c3 TINYINT,
                  |c4 BOOLEAN,
                  |c5 SMALLINT,
                  |c6 MEDIUMINT,
                  |c7 INTEGER,
                  |c8 BIGINT,
                  |c9 FLOAT,
                  |c10 DOUBLE,
                  |c11 DECIMAL,
                  |c12 DATE,
                  |c13 DATETIME,
                  |c14 TIMESTAMP,
                  |c15 TIME,
                  |c16 YEAR,
                  |c17 CHAR(64),
                  |c18 VARCHAR(64),
                  |c19 BINARY(64),
                  |c20 VARBINARY(64),
                  |c21 TINYBLOB,
                  |c22 TINYTEXT,
                  |c23 BLOB,
                  |c24 TEXT,
                  |c25 MEDIUMBLOB,
                  |c26 MEDIUMTEXT,
                  |c27 LONGBLOB,
                  |c28 LONGTEXT,
                  |c29 ENUM('male' , 'female' , 'both' , 'unknow'),
                  |c30 SET('a', 'b', 'c'),
                  |c31 INT UNSIGNED,
                  |c32 BIGINT UNSIGNED
                  |)
      """.stripMargin)

    jdbcUpdate(s"""
                  |insert into $dbtable values(
                  |1,
                  |B'1',
                  |B'01111100',
                  |29,
                  |1,
                  |29,
                  |29,
                  |29,
                  |29,
                  |29.9,
                  |29.9,
                  |29.9,
                  |'2019-01-01',
                  |'2019-01-01 11:11:11',
                  |'2019-01-01 11:11:11',
                  |'11:11:11',
                  |'2019',
                  |'char test',
                  |'varchar test',
                  |'binary test',
                  |'varbinary test',
                  |'tinyblob test',
                  |'tinytext test',
                  |'blob test',
                  |'text test',
                  |'mediumblob test',
                  |'mediumtext test',
                  |'longblob test',
                  |'longtext test',
                  |'male',
                  |'a,b',
                  |299,
                  |2999
                  |)
       """.stripMargin)
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
