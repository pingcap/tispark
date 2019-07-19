package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, _}

/**
 * DECIMAL type include:
 * 1. DECIMAL
 */
class DecimalOverflowSuite extends BaseDataSourceTest("test_data_type_decimal_overflow") {

  case class TestData(length: Int, precision: Int, writeData: Double, readData: Long) {}

  test("Test DECIMAL Not Overflow") {
    val testDataList =
      TestData(38, 0, 1.5d, 2L) ::
        TestData(38, 0, 1.4d, 1L) ::
        TestData(38, 0, -1.5d, -2L) ::
        TestData(38, 0, -1.4d, -1L) ::
        TestData(38, 1, 1.5d, 15L) ::
        TestData(38, 1, 1.4d, 14L) ::
        TestData(38, 1, -1.5d, -15L) ::
        TestData(38, 1, -1.4d, -14L) ::
        TestData(38, 2, 1.5d, 150L) ::
        TestData(38, 2, 1.4d, 140L) ::
        TestData(38, 2, -1.5d, -150L) ::
        TestData(38, 2, -1.4d, -140L) ::
        TestData(38, 10, 1.4d, 1.4E10d.toLong) ::
        TestData(38, 10, 1.5d, 1.5E10d.toLong) ::
        TestData(38, 10, -1.4d, -1.4E10d.toLong) ::
        TestData(38, 10, -1.5d, -1.5E10d.toLong) ::
        TestData(10, 4, 999999.9999d, 9999999999L) ::
        Nil

    testDataList.foreach { testData =>
      compareTiDBWriteWithJDBC {
        case (writeFunc, _) =>
          dropTable()

          jdbcUpdate(
            s"create table $dbtable(i INT, c1 DECIMAL(${testData.length}, ${testData.precision}))"
          )

          val row1 = Row(1, testData.writeData)
          val schema = StructType(
            List(
              StructField("i", IntegerType),
              StructField("c1", DoubleType)
            )
          )

          val readRow1 = Row(1, java.math.BigDecimal.valueOf(testData.readData, testData.precision))
          val readSchema = StructType(
            List(
              StructField("i", IntegerType),
              StructField("c1", DecimalType(testData.length, testData.precision))
            )
          )

          writeFunc(List(row1), schema, None)
          compareTiDBSelectWithJDBC(Seq(readRow1), readSchema)
      }
    }
  }

  case class OverflowTestData(length: Int, precision: Int, writeData: Double) {}

  test("Test DECIMAL Overflow") {
    val testDataList =
      OverflowTestData(10, 4, 1000000d) ::
        OverflowTestData(2, 0, 100d) ::
        OverflowTestData(4, 0, 10000d) ::
        Nil

    testDataList.foreach { testData =>
      dropTable()

      jdbcUpdate(
        s"create table $dbtable(i INT, c1 DECIMAL(${testData.length}, ${testData.precision}))"
      )

      val row1 = Row(1, testData.writeData)
      val schema = StructType(
        List(
          StructField("i", IntegerType),
          StructField("c1", DoubleType)
        )
      )

      val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
      val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
      val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
      val tidbErrorMsg = "Out of range"

      compareTiDBWriteFailureWithJDBC(
        List(row1),
        schema,
        jdbcErrorClass,
        jdbcErrorMsg,
        tidbErrorClass,
        tidbErrorMsg
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
