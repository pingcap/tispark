/*
 *
 * Copyright 2019 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.sql.test.generator

import com.pingcap.tikv.row.ObjectRowImpl
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.generator.DataType._

import scala.collection.mutable
import scala.util.Random

object TestDataGenerator {
  type TiRow = com.pingcap.tikv.row.Row

  val bits = List(BIT)
  val booleans = List(BOOLEAN)
  val integers = List(TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT)
  val decimals = List(DECIMAL)
  val doubles = List(FLOAT, DOUBLE)

  val timestamps = List(TIMESTAMP, DATETIME)
  val dates = List(DATE)
  val durations = List(TIME)
  val years = List(YEAR)

  val texts = List(TEXT, TINYTEXT, MEDIUMTEXT, LONGTEXT)
  val strings = List(VARCHAR, CHAR)
  val binaries = List(VARBINARY, BINARY)
  val bytes = List(BLOB, TINYBLOB, MEDIUMBLOB, LONGBLOB)
  val enums = List(ENUM)
  val sets = List(SET)

  val jsons = List(JSON)

  // M indicates the number of digits
  val withM: List[ReflectedDataType] = bits ::: integers ::: decimals ::: doubles
  // D indicates the number of digits after the decimal point
  val withD: List[ReflectedDataType] = decimals ::: doubles
  // U indicates whether the datatype is unsigned
  val withU: List[ReflectedDataType] = integers ::: decimals ::: doubles
  // F indicates the fraction of date/datetime format
  val withF: List[ReflectedDataType] = timestamps ::: durations

  val numeric: List[ReflectedDataType] = bits ::: booleans ::: integers ::: decimals ::: doubles
  val dateAndDateTime: List[ReflectedDataType] = timestamps ::: dates ::: durations ::: years

  val stringAndBinaries: List[ReflectedDataType] = strings ::: binaries
  val charCharset: List[ReflectedDataType] = strings ::: texts
  val binaryCharset: List[ReflectedDataType] = binaries ::: bytes
  val enumAndSets: List[ReflectedDataType] = enums ::: sets
  // val stringType: List[ReflectedDataType] = texts ::: strings ::: binaries ::: enumAndSets
  val stringType: List[ReflectedDataType] = charCharset ::: binaryCharset
  val varString: List[ReflectedDataType] = List(VARCHAR, VARBINARY)

  val unsignedType: List[ReflectedDataType] = numeric

  // TODO: support json
  val allDataTypes: List[ReflectedDataType] =
    numeric ::: dateAndDateTime ::: stringType ::: enumAndSets
  // supported data types for generator
  val supportedDataTypes: List[ReflectedDataType] = allDataTypes

  // basic data types that represent each type category
  val baseDataTypes: List[ReflectedDataType] =
    List(BIT, BOOLEAN, INT, BIGINT, DECIMAL, DOUBLE, TIMESTAMP, DATE, TEXT, VARCHAR, BLOB)

  // data types used in TPC-H tests
  val tpchDataTypes: List[ReflectedDataType] = List(INT, DECIMAL, DATE, VARCHAR, CHAR)

  ///////////////////// Functions to Judge Catalog of DataType ////////////////////////
  //  def isBits(dataType: DataType): Boolean = bits.contains(dataType)
  //  def isBooleans(dataType: DataType): Boolean = booleans.contains(dataType)
  //  def isIntegers(dataType: DataType): Boolean = integers.contains(dataType)
  def isDecimals(dataType: ReflectedDataType): Boolean = decimals.contains(dataType)

  def isDoubles(dataType: ReflectedDataType): Boolean = doubles.contains(dataType)

  //  def isTimestamps(dataType: DataType): Boolean = timestamps.contains(dataType)
  //  def isDates(dataType: DataType): Boolean = dates.contains(dataType)
  //  def isDurations(dataType: DataType): Boolean = durations.contains(dataType)
  //  def isYears(dataType: DataType): Boolean = years.contains(dataType)
  //  def isTexts(dataType: DataType): Boolean = texts.contains(dataType)
  //  def isStrings(dataType: DataType): Boolean = strings.contains(dataType)
  //  def isBinaries(dataType: DataType): Boolean = binaries.contains(dataType)
  //  def isBytes(dataType: DataType): Boolean = bytes.contains(dataType)
  //  def isEnums(dataType: DataType): Boolean = enums.contains(dataType)
  //  def isSets(dataType: DataType): Boolean = sets.contains(dataType)
  //  def isJsons(dataType: DataType): Boolean = jsons.contains(dataType)
  def isNumeric(dataType: ReflectedDataType): Boolean = numeric.contains(dataType)

  def isStringType(dataType: ReflectedDataType): Boolean = stringType.contains(dataType)

  def isCharCharset(dataType: ReflectedDataType): Boolean = charCharset.contains(dataType)

  def isBinaryCharset(dataType: ReflectedDataType): Boolean = binaryCharset.contains(dataType)

  def isCharOrBinary(dataType: ReflectedDataType): Boolean = stringAndBinaries.contains(dataType)

  def isEnumOrSet(dataType: ReflectedDataType): Boolean = enumAndSets.contains(dataType)

  def getLength(dataType: TiDataType): Long =
    if (dataType.isLengthUnSpecified) {
      dataType.getDefaultLength
    } else {
      dataType.getLength
    }

  def getDecimal(dataType: TiDataType): Int = dataType.getDecimal

  /**
   * SchemaGenerator generates a schema from input info.
   *
   * code example for schema
   * {{{
   *   CREATE TABLE `tispark_test`.`test_table` (
   *     `col_int0` int not null,
   *     `col_int1` int default null,
   *     `col_double` double not null default 0.2,
   *     `col_varchar` varchar(50) default null,
   *     `col_decimal` decimal(20,3) default null,
   *     PRIMARY KEY (`col_int0`),
   *     KEY `idx_col_int1_col_double`(`col_int1`,`col_double`),
   *     KEY `idx_col_varchar`(`col_varchar`(20)),
   *     KEY `idx_col_double_col_decimal`(`col_double`,`col_decimal`)
   *   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   * }}}
   * will be
   * {{{
   *   schemaGenerator(
   *     "tispark_test",
   *     "test_table",
   *     List(
   *       (INT, "", "not null"),
   *       (INT, "", "default null"),
   *       (DOUBLE, "", "not null default 0.2"),
   *       (VARCHAR, "50", "default null"),
   *       (DECIMAL, "20,3", "default null")
   *     ),
   *     List(
   *       PrimaryKey(List(DefaultColumn(1)))
   *       Key(List(DefaultColumn(2), DefaultColumn(3))),
   *       Key(List(PrefixColumn(4, 20))),
   *       Key(List(DefaultColumn(3), DefaultColumn(5)))
   *     ))
   * }}}
   *
   * @param database                 database name
   * @param table                    table name
   * @param r                        random
   * @param dataTypesWithDescription (typeName, lengthDescriptions, extraDescriptions)
   * @param indices                  index info, list of column ids chosen (start from 1)
   * @return Generated Schema
   */
  def schemaGenerator(
      database: String,
      table: String,
      r: Random,
      dataTypesWithDescription: List[(ReflectedDataType, String, String)],
      indices: List[Index]): Schema = {

    // validation
    assert(
      dataTypesWithDescription.forall(x => supportedDataTypes.contains(x._1)),
      "required data type not present for generator")
    assert(
      indices
        .forall(_.indexColumns.forall(x =>
          x.getId >= 0 && x.getId < dataTypesWithDescription.size)))

    val dataTypeList: List[ReflectedDataType] = dataTypesWithDescription.map {
      _._1
    }

    val dataTypeMap: mutable.Map[ReflectedDataType, Int] =
      new mutable.HashMap[ReflectedDataType, Int].withDefaultValue(0)
    val dataTypeCountMap: mutable.Map[ReflectedDataType, Int] =
      new mutable.HashMap[ReflectedDataType, Int].withDefaultValue(0)

    val columnNameToDataTypeMap: mutable.Map[String, (ReflectedDataType, String, String)] =
      new mutable.HashMap[String, (ReflectedDataType, String, String)]

    dataTypeList.foreach { x =>
      dataTypeMap(x) += 1
    }

    val columnNames: List[String] =
      dataTypesWithDescription.map { x =>
        val tp = x._1
        val ret = if (dataTypeMap(tp) == 1) {
          generateColumnName(tp)
        } else {
          val cnt = dataTypeCountMap(tp)
          dataTypeCountMap(tp) += 1
          generateColumnName(tp, cnt)
        }
        columnNameToDataTypeMap(ret) = x
        ret
      }

    def extractFromTypeDesc(str: String): (Integer, Integer) = {
      if (str.isEmpty) {
        (null, null)
      } else {
        val s = str.split(",").map(_.toInt)
        assert(s.nonEmpty && s.length <= 2, "type desc is not valid")
        if (s.length == 1) {
          (s(0), null)
        } else {
          (s(0), s(1))
        }
      }
    }

    def buildColumnDesc(
        dataType: ReflectedDataType,
        r: Random,
        typeDesc: String,
        desc: String): (ReflectedDataType, (Integer, Integer), String) = {
      var (m, d) = extractFromTypeDesc(typeDesc)
      if (m == -1 && isVarString(dataType)) {
        assert(d == null, "TEXT/BLOB should/must have only length specified")
        m = r.nextInt(40) + 10
      }
      (dataType, (m, d), desc)
    }

    val columnDesc: mutable.Map[String, (ReflectedDataType, (Integer, Integer), String)] = {
      columnNameToDataTypeMap.map { x =>
        (x._1, buildColumnDesc(x._2._1, r, x._2._2, x._2._3))
      }
    }

    // single column primary key defined in schema
    val singleColumnPrimaryKey: Map[String, (List[(String, Integer)], Boolean, Boolean)] =
      columnDesc.toMap
        .filter { colDesc =>
          colDesc._2._3.contains("primary key")
        }
        .map { x =>
          (x._1, (List((x._1, null)), true, true))
        }

    assert(singleColumnPrimaryKey.size <= 1, "More than one primary key present in schema")

    val idxColumns: Map[String, (List[(String, Integer)], Boolean, Boolean)] =
      singleColumnPrimaryKey ++
        indices.map { idx =>
          val columns = idx.indexColumns.map(x => (columnNames(x.getId), x.getLength))
          if (idx.isPrimaryKey && idx.indexColumns.lengthCompare(1) == 0) {
            val pkColumn = columnNames(idx.indexColumns.head.getId)
            val x = columnDesc(pkColumn)
            columnDesc(pkColumn) = (x._1, x._2, x._3 + " not null")
          }
          (
            generateIndexName(columns.map {
              _._1
            }),
            (columns, idx.isPrimaryKey, idx.isUnique))
        }.toMap

    Schema(database, table, columnNames, columnDesc.toMap, idxColumns)
  }

  def isVarString(dataType: ReflectedDataType): Boolean = varString.contains(dataType)

  // Generating names
  def generateColumnName(dataType: ReflectedDataType) = s"col_$dataType"

  def generateColumnName(dataType: ReflectedDataType, num: Int) = s"col_$dataType$num"

  // Schema Generator

  def generateIndexName(columns: List[String]): String = "idx_" + columns.mkString("_")

  def hash(value: Any, len: Int = -1): String =
    value match {
      case null => "null"
      case (v: Any, l: Int) => hash(v, l)
      case list: List[Any] =>
        val ret = StringBuilder.newBuilder
        ret ++= "("
        for (i <- list.indices) {
          if (i > 0) ret ++= ","
          val elem = list(i).asInstanceOf[(Any, Int)]
          ret ++= hash(elem._1, elem._2)
        }
        ret ++= ")"
        ret.toString
      case b: Array[Boolean] =>
        if (len != -1) {
          b.slice(0, len).mkString("[", ",", "]")
        } else {
          b.mkString("[", ",", "]")
        }
      case b: Array[Byte] =>
        if (len != -1) {
          b.slice(0, len).mkString("[", ",", "]")
        } else {
          b.mkString("[", ",", "]")
        }
      case t: java.sql.Timestamp =>
        // timestamp was indexed as Integer when treated as unique key
        s"${t.getTime / 1000}"
      case s: String if len != -1 => s.slice(0, len)
      case x if len == -1 => x.toString
      case _ => throw new RuntimeException(s"hash method for value $value not found!")
    }

  // value may be (Any, Int) or List[(Any, Int)], in this case, it means
  // the value to be hashed is a list of/one unique value(s) with prefix length.
  def checkUnique(value: Any, set: mutable.Set[String]): Boolean = {
    val hashedValue = hash(value)
    if (!set.apply(hashedValue)) {
      set += hashedValue
      true
    } else {
      false
    }
  }

  // Data Generator
  def randomDataGenerator(schema: Schema, rowCount: Long, directory: String, r: Random): Data = {
    Data(schema, generateRandomRows(schema, rowCount, r), directory)
  }

  def generateRandomRows(schema: Schema, n: Long, r: Random): List[TiRow] = {
    // offset of pk columns
    val pkOffset: List[(Int, Int)] = {
      val primary = schema.indexInfo.filter(_.isPrimary)
      if (primary.nonEmpty && primary.lengthCompare(1) == 0) {
        primary.head.indexColumns.map(
          x =>
            (
              schema.columnNames.indexOf(x.column),
              if (x.length == null) -1
              else x.length.intValue()))
      } else {
        List.empty[(Int, Int)]
      }
    }
    schema.columnInfo.indices.foreach { i =>
      schema.columnInfo(i).generator.reset()
      pkOffset.find(_._1 == i) match {
        case Some((_, len)) =>
          schema.columnInfo(i).generator.preGenerateRandomValues(r, n, len)
        case None =>
          schema.columnInfo(i).generator.preGenerateRandomValues(r, n)
      }
    }

    (1.toLong to n).map { _ =>
      generateRandomRow(schema, r)
    }.toList
  }

  private def generateRandomRow(schema: Schema, r: Random): TiRow = {
    val length = schema.columnInfo.length
    val row: TiRow = ObjectRowImpl.create(length)
    for (i <- schema.columnInfo.indices) {
      val columnInfo = schema.columnInfo(i)
      generateRandomColValue(row, i, r, columnInfo.generator)
    }
    row
  }

  private def generateRandomColValue(
      row: TiRow,
      offset: Int,
      r: Random,
      colValueGenerator: ColumnValueGenerator): Unit = {
    val value = colValueGenerator.next(r)
    if (value == null) {
      row.setNull(offset)
    } else {
      row.set(offset, colValueGenerator.tiDataType, value)
    }
  }
}

class TestDataGenerator extends SparkFunSuite {

  test("base test for schema generator") {
    val r = new Random(1234)
    val schema = TestDataGenerator.schemaGenerator(
      "tispark_test",
      "test_table",
      r,
      List(
        (INT, "", "not null primary key"),
        (INT, "", "default null"),
        (DOUBLE, "", "not null default 0.2"),
        (VARCHAR, "50", "default null"),
        (DECIMAL, "20,3", "default null")),
      List(
        Key(List(DefaultColumn(2), DefaultColumn(3))),
        Key(List(PrefixColumn(4, 20))),
        Key(List(DefaultColumn(3), DefaultColumn(5)))))
    val schema2 = TestDataGenerator.schemaGenerator(
      "tispark_test",
      "test_table",
      r,
      List(
        (INT, "", "not null"),
        (INT, "", "default null"),
        (DOUBLE, "", "not null default 0.2"),
        (VARCHAR, "50", "default null"),
        (DECIMAL, "20,3", "default null")),
      List(
        PrimaryKey(List(DefaultColumn(1))),
        Key(List(DefaultColumn(2), DefaultColumn(3))),
        Key(List(PrefixColumn(4, 20))),
        Key(List(DefaultColumn(3), DefaultColumn(5)))))
    val answer =
      """CREATE TABLE `tispark_test`.`test_table` (
        |  `col_int0` int not null,
        |  `col_int1` int default null,
        |  `col_double` double not null default 0.2,
        |  `col_varchar` varchar(50) default null,
        |  `col_decimal` decimal(20,3) default null,
        |  PRIMARY KEY (`col_int0`),
        |  KEY `idx_col_int1_col_double`(`col_int1`,`col_double`),
        |  KEY `idx_col_varchar`(`col_varchar`(20)),
        |  KEY `idx_col_double_col_decimal`(`col_double`,`col_decimal`)
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""".stripMargin
    assert(schema.toString === answer)
    assert(schema2.toString === answer)
  }

  test("test generate schema") {
    val r = new Random(1234)
    val schema = TestDataGenerator.schemaGenerator(
      "tispark_test",
      "test_table",
      r,
      List(
        (INT, "", "not null primary key"),
        (INT, "", "default null"),
        (BIT, "3", "default null"),
        (DOUBLE, "", "not null default 0.2"),
        (VARCHAR, "50", "default null"),
        (DECIMAL, "10,3", "default null")),
      List(
        Key(List(DefaultColumn(2), DefaultColumn(4))),
        Key(List(PrefixColumn(5, 20))),
        Key(List(DefaultColumn(4), DefaultColumn(5)))))
    val data: Data = TestDataGenerator.randomDataGenerator(schema, 10, "tispark-test", r)
    data.save()
  }
}
