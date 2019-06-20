package com.pingcap.tispark.datatype

import com.pingcap.tispark.datasource.BaseDataSourceTest

class DataTypeSuite extends BaseDataSourceTest("test_data_type") {

  override protected val database: String = "test"
  override protected val dbtable = s"$database.test_data_type"

  test("Test Read different types") {

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
                  |c30 SET('a', 'b', 'c')
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
                  |'a,b'
                  |)
       """.stripMargin)
    val tiTableInfo = ti.tiSession.getCatalog.getTable(s"$dbPrefix$database", table)
    for (i <- 0 until tiTableInfo.getColumns.size()) {
      println(s"$i -> ${tiTableInfo.getColumn(i).getType}")
    }
    assert(tiTableInfo != null)

    // compareTiDBSelectWithJDBC_V2()
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
