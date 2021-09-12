package org.apache.spark.sql.types

import com.pingcap.tispark.test.RandomTest
import com.pingcap.tispark.test.generator.DataGenerator.{isNumeric, isStringType}
import com.pingcap.tispark.test.generator.DataType.{BOOLEAN, ReflectedDataType, TINYINT}
import com.pingcap.tispark.test.generator.SchemaAndData
import org.apache.spark.sql.BaseTiSparkTest

import scala.util.Random

trait BaseRandomDataTypeTest extends BaseTiSparkTest with RandomTest {
  protected val r: Random = new Random(generateDataSeed)

  protected def rowCount: Int = 10

  protected val database: String

  override def afterAll(): Unit = {
    try {
      tidbStmt.execute(s"drop database $database")
    } catch {
      case _: Throwable =>
    } finally {
      super.afterAll()
    }
  }

  protected def loadToDB(schemaAndData: SchemaAndData): Unit = {
    val initSQLList = schemaAndData.getInitSQLList
    initSQLList.foreach { sql =>
      try {
        tidbStmt.execute(sql)
        println(sql)
      } catch {
        case _: Throwable =>
      }
    }
  }

  private val cmps: List[String] = List(">", "<")
  private val eqs: List[String] = List("=", "<>")

  implicit class C[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]): Traversable[(X, Y)] = for { x <- xs; y <- ys } yield (x, y)
  }

  protected def simpleSelect(
      dbName: String,
      tableName: String,
      col1: String,
      col2: String,
      dataType: ReflectedDataType): Unit = {
    setCurrentDatabase(dbName)

    if (enableTiFlashTest) {
      checkLoadTiFlashWithRetry(tableName, Some(dbName))
    }

    for ((op, value) <- getOperations(dataType)) {
      val query = s"select $col1 from $tableName where $col2 $op $value"
      println(query)
      runTest(query, skipJDBC = true, canTestTiFlash = true)
    }
  }

  private def getOperations(dataType: ReflectedDataType): List[(String, String)] = {
    List(("is", "null")) ++ {
      (cmps ++ eqs) cross {
        dataType match {
          case TINYINT => List("1", "0")
          case _ if isNumeric(dataType) => List("1", "2333")
          case _ if isStringType(dataType) => List("\'PingCAP\'", "\'\'")
          case _ => List.empty[String]
        }
      }
    } ++ {
      eqs cross {
        dataType match {
          case BOOLEAN => List("false", "true")
          case _ => List.empty[String]
        }
      }
    }
  }
}
