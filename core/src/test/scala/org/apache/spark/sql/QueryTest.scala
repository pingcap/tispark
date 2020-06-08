/*
 *
 * Copyright 2017 PingCAP, Inc.
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

package org.apache.spark.sql

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.types.StructField

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

abstract class QueryTest extends SparkFunSuite {

  private val eps = 1.0e-2

  def listToString(result: List[List[Any]]): String =
    if (result == null) s"[len: null] = null"
    else if (result.isEmpty) s"[len: 0] = Empty"
    else s"[len: ${result.length}] = ${result.map(mapStringList).mkString(",")}"

  private def mapStringList(result: List[Any]): String =
    if (result == null) "null" else "List(" + result.map(mapString).mkString(",") + ")"

  private def mapString(result: Any): String =
    if (result == null) "null"
    else
      result match {
        case _: Array[Byte] =>
          var str = "["
          for (s <- result.asInstanceOf[Array[Byte]]) {
            str += " " + s.toString
          }
          str += " ]"
          str
        case _ =>
          result.toString
      }

  /**
   * Runs the plan and makes sure the answer contains all of the keywords.
   */
  def checkKeywordsExist(df: DataFrame, keywords: String*): Unit = {
    val outputs = df.collect().map(_.mkString).mkString
    for (key <- keywords) {
      assert(outputs.contains(key), s"Failed for $df ($key doesn't exist in result)")
    }
  }

  /**
   * Runs the plan and makes sure the answer does NOT contain any of the keywords.
   */
  def checkKeywordsNotExist(df: DataFrame, keywords: String*): Unit = {
    val outputs = df.collect().map(_.mkString).mkString
    for (key <- keywords) {
      assert(!outputs.contains(key), s"Failed for $df ($key existed in the result)")
    }
  }

  /**
   * Asserts that a given [[Dataset]] will be executed using the given number of cached results.
   */
  def assertCached(query: Dataset[_], numCachedTables: Int = 1): Unit = {
    val planWithCaching = query.queryExecution.withCachedData
    val cachedData = planWithCaching collect {
      case cached: InMemoryRelation => cached
    }

    assert(
      cachedData.lengthCompare(numCachedTables) == 0,
      s"Expected query to contain $numCachedTables, but it actually had ${cachedData.size}\n" +
        planWithCaching)
  }

  protected def spark: SparkSession

  protected def compSqlResult(
      sql: String,
      lhs: List[List[Any]],
      rhs: List[List[Any]],
      checkLimit: Boolean = true): Boolean = {
    val isOrdered = sql.contains(" order by ")
    val isLimited = sql.contains(" limit ")

    if (checkLimit && !isOrdered && isLimited) {
      logger.warn(
        s"Unknown correctness of test result: sql contains \'limit\' but not \'order by\'.")
      false
    } else {
      compResult(lhs, rhs, isOrdered)
    }
  }

  /**
   * Compare whether lhs equals to rhs
   *
   * @param isOrdered whether the input data `lhs` and `rhs` should be compared in order
   * @return true if results are the same
   */
  protected def compResult(
      lhs: List[List[Any]],
      rhs: List[List[Any]],
      isOrdered: Boolean = true): Boolean = {
    // If cannot be converted to double, it is also not comparable.
    def toDouble(x: Any): Double =
      x match {
        case d: Double => d
        case d: Float => d.toDouble
        case d: java.math.BigDecimal => d.doubleValue()
        case d: BigDecimal => d.bigDecimal.doubleValue()
        case d: Number => d.doubleValue()
        case d: String => BigDecimal(d).doubleValue()
        case d: Boolean => if (d) 1d else 0d
        case _ => Double.NaN
      }

    def toInteger(x: Any): Long =
      x match {
        case d: BigInt => d.bigInteger.longValue()
        case d: Number => d.longValue()
        case d: Boolean => if (d) 1L else 0L
        case d: Array[Byte] =>
          if (d.length > 8) {
            0L
          } else {
            var r = 0L
            for (x <- d) {
              r = r * 256 + (if (x >= 0) { x }
                             else { 256 + x })
            }
            r
          }
      }

    def toString(value: Any): String =
      new SimpleDateFormat("yy-MM-dd HH:mm:ss").format(value)

    def compNull(l: Any, r: Any): Boolean = {
      if (l == null) {
        if (r == null || r.toString.equalsIgnoreCase("null")) {
          return true
        }
      } else if (l.toString.equalsIgnoreCase("null")) {
        if (r == null || r.toString.equalsIgnoreCase("null")) {
          return true
        }
      }
      false
    }

    def compValue(lhs: Any, rhs: Any): Boolean = {
      if (lhs == rhs || compNull(lhs, rhs) || lhs.toString == rhs.toString) {
        true
      } else
        lhs match {
          case _: Array[Byte] if rhs.isInstanceOf[Array[Byte]] =>
            val l = lhs.asInstanceOf[Array[Byte]]
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
          case _: Array[Byte] if rhs.isInstanceOf[Long] =>
            val l = lhs.asInstanceOf[Array[Byte]].toList.reverse
            val r = rhs.asInstanceOf[Long]
            if (l.lengthCompare(8) > 0) {
              false
            } else {
              var eq = true
              for (i <- l.indices) {
                val a = (((255L << (i * 8)) & r) >> (i * 8)).toByte
                if (!l(i).equals(a)) {
                  eq = false
                }
              }
              eq
            }

          case _: Double | _: Float | _: BigDecimal | _: java.math.BigDecimal =>
            val l = toDouble(lhs)
            val r = toDouble(rhs)
            Math.abs(l - r) < eps || Math.abs(r) > eps && Math.abs((l - r) / r) < eps
          case _: Number | _: BigInt | _: java.math.BigInteger | _: Boolean =>
            toInteger(lhs) == toInteger(rhs)
          case _: Timestamp =>
            toString(lhs) == toString(rhs)
          case _ =>
            false
        }
    }

    def compRow(lhs: List[Any], rhs: List[Any]): Boolean =
      if (lhs == null && rhs == null) {
        true
      } else if (lhs == null || rhs == null) {
        false
      } else if (lhs.lengthCompare(rhs.length) != 0) {
        false
      } else {
        !lhs.zipWithIndex.exists {
          case (value, i) => !compValue(value, rhs(i))
        }
      }

    def comp(query: List[List[Any]], result: List[List[Any]]): Boolean =
      !result.zipWithIndex.exists {
        case (row, i) => !compRow(row, query(i))
      }

    def toComparableString(row: List[Any]): String = {
      row.map {
        case null => "null"
        case a: Array[Byte] => a.mkString("[", ",", "]")
        case b: Boolean => if (b) "1" else "0"
        case s => s.toString
      }.mkString
    }

    def sortComp(l: List[Any], r: List[Any]): Boolean =
      toComparableString(l).compareTo(toComparableString(r)) < 0

    if (lhs != null && rhs != null) {
      try {
        if (lhs.lengthCompare(rhs.length) != 0) {
          false
        } else {
          // the result order may be different in TiDB and TiSpark, so compare result as follows
          // 1. compare directly
          // 2. if 1 fails, use `NullableListOrdering` to sort the result, then compare
          // 3. if 2 fails and the sql does not contains `order by`, use string based `sortComp` to sort the result, then compare
          var result = comp(lhs, rhs)
          if (!result) {
            implicit object NullableListOrdering extends Ordering[List[Any]] {
              override def compare(p1: List[Any], p2: List[Any]): Int =
                p1.contains(null).compareTo(p2.contains(null))
            }
            result = comp(lhs.sortBy[List[Any]](x => x), rhs.sortBy[List[Any]](x => x))
          }
          if (!result && !isOrdered) {
            result = comp(
              lhs.sortWith((_1, _2) => sortComp(_1, _2)),
              rhs.sortWith((_1, _2) => sortComp(_1, _2)))
          }
          result
        }
      } catch {
        // TODO:Remove this temporary exception handling
        //      case _:RuntimeException => false
        case e: Throwable =>
          logger.warn("Comparison failed due to exception: " + e.getMessage)
          false
      }
    } else {
      false
    }
  }

  protected def dfData(df: DataFrame, schema: scala.Array[StructField]): List[List[Any]] =
    df.collect()
      .map(row => {
        val rowRes = ArrayBuffer.empty[Any]
        for (i <- 0 until row.length) {
          if (row.get(i) == null) {
            rowRes += null
          } else {
            rowRes += toOutput(row.get(i), schema(i).dataType.typeName)
          }
        }
        rowRes.toList
      })
      .toList

  protected def toOutput(value: Any, colType: String): Any =
    value match {
      case _: BigDecimal =>
        value.asInstanceOf[BigDecimal].setScale(2, BigDecimal.RoundingMode.HALF_UP)
      case _: Date if colType.equalsIgnoreCase("YEAR") =>
        value.toString.split("-")(0)
      case default =>
        default
    }

  /**
   * Evaluates a dataset to make sure that the result of calling collect matches the given
   * expected answer.
   */
  protected def checkDataset[T](ds: => Dataset[T], expectedAnswer: T*): Unit = {
    val result = getResult(ds)

    if (!compare(result.toSeq, expectedAnswer)) {
      fail(s"""
              |Decoded objects do not match expected objects:
              |expected: $expectedAnswer
              |actual:   ${result.toSeq}
              |${ds.exprEnc.deserializer.treeString}
         """.stripMargin)
    }
  }

  /**
   * Evaluates a dataset to make sure that the result of calling collect matches the given
   * expected answer, after sort.
   */
  protected def checkDatasetUnorderly[T: Ordering](
      ds: => Dataset[T],
      expectedAnswer: T*): Unit = {
    val result = getResult(ds)

    if (!compare(result.toSeq.sorted, expectedAnswer.sorted)) {
      fail(s"""
              |Decoded objects do not match expected objects:
              |expected: $expectedAnswer
              |actual:   ${result.toSeq}
              |${ds.exprEnc.deserializer.treeString}
         """.stripMargin)
    }
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Row): Unit =
    checkAnswer(df, Seq(expectedAnswer))

  protected def checkAnswer(df: => DataFrame, expectedAnswer: DataFrame): Unit =
    checkAnswer(df, expectedAnswer.collect())

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   *
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF =
      try df
      catch {
        case ae: AnalysisException =>
          if (ae.plan.isDefined) {
            fail(s"""
                    |Failed to analyze query: $ae
                    |${ae.plan.get}
                    |
                    |${stackTraceToString(ae)}
                    |""".stripMargin)
          } else {
            throw ae
          }
      }

    assertEmptyMissingInput(analyzedDF)

    QueryTest.checkAnswer(analyzedDF, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  /**
   * Asserts that a given [[Dataset]] does not have missing inputs in all the analyzed plans.
   */
  def assertEmptyMissingInput(query: Dataset[_]): Unit = {
    assert(
      query.queryExecution.analyzed.missingInput.isEmpty,
      s"The analyzed logical plan has missing inputs:\n${query.queryExecution.analyzed}")
    assert(
      query.queryExecution.optimizedPlan.missingInput.isEmpty,
      s"The optimized logical plan has missing inputs:\n${query.queryExecution.optimizedPlan}")
    assert(
      query.queryExecution.executedPlan.missingInput.isEmpty,
      s"The physical plan has missing inputs:\n${query.queryExecution.executedPlan}")
  }

  protected def checkAggregatesWithTol(
      dataFrame: DataFrame,
      expectedAnswer: Row,
      absTol: Double): Unit =
    checkAggregatesWithTol(dataFrame, Seq(expectedAnswer), absTol)

  /**
   * Runs the plan and makes sure the answer is within absTol of the expected result.
   *
   * @param dataFrame the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param absTol the absolute tolerance between actual and expected answers.
   */
  protected def checkAggregatesWithTol(
      dataFrame: DataFrame,
      expectedAnswer: Seq[Row],
      absTol: Double): Unit = {
    // TODO: catch exceptions in data frame execution
    val actualAnswer = dataFrame.collect()
    require(
      actualAnswer.length == expectedAnswer.length,
      s"actual num rows ${actualAnswer.length} != expected num of rows ${expectedAnswer.length}")

    actualAnswer.zip(expectedAnswer).foreach {
      case (actualRow, expectedRow) =>
        QueryTest.checkAggregatesWithTol(actualRow, expectedRow, absTol)
    }
  }

  protected def callWithRetry[A](execute: => A): A = {
    callWithRetry(execute, retryOnFailure = 3)
  }

  /**
   * Execute a command with retry number = `retryOnFailure`
   *
   * @param execute command that returns anything
   * @param retryOnFailure number of remaining retries before it fails
   * @param exception last exception thrown
   * @return result of command
   */
  protected def callWithRetry[A](
      execute: => A,
      retryOnFailure: Int,
      exception: Exception = null): A = {
    if (retryOnFailure <= 0) {
      logger.error("callWithRetry failure", exception)
      fail(exception)
    } else
      try {
        execute
      } catch {
        case e: Exception =>
          logger.info(s"Error occurs when calling with retry, remain retries: $retryOnFailure", e)
          callWithRetry(execute, retryOnFailure - 1, e)
      }
  }

  private def getResult[T](ds: => Dataset[T]): Array[T] = {
    val analyzedDS =
      try ds
      catch {
        case ae: AnalysisException =>
          if (ae.plan.isDefined) {
            fail(s"""
                    |Failed to analyze query: $ae
                    |${ae.plan.get}
                    |
                    |${stackTraceToString(ae)}
             """.stripMargin)
          } else {
            throw ae
          }
      }
    assertEmptyMissingInput(analyzedDS)

    try ds.collect()
    catch {
      case e: Exception =>
        fail(
          s"""
             |Exception collecting dataset as objects
             |${ds.exprEnc}
             |${ds.exprEnc.deserializer.treeString}
             |${ds.queryExecution}
           """.stripMargin,
          e)
    }
  }

  private def compare(obj1: Any, obj2: Any): Boolean =
    (obj1, obj2) match {
      case (null, null) => true
      case (null, _) => false
      case (_, null) => false
      case (a: Array[_], b: Array[_]) =>
        a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r) }
      case (a: Iterable[_], b: Iterable[_]) =>
        a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r) }
      case (a, b) => a == b
    }
}

object QueryTest {

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * If there was exception during the execution or the contents of the DataFrame does not
   * match the expected result, an error message will be returned. Otherwise, a [[None]] will
   * be returned.
   *
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param checkToRDD whether to verify deserialization to an RDD. This runs the query twice.
   */
  def checkAnswer(
      df: DataFrame,
      expectedAnswer: Seq[Row],
      checkToRDD: Boolean = true): Option[String] = {
    val isSorted = df.logicalPlan.collect { case s: logical.Sort => s }.nonEmpty
    if (checkToRDD) {
      df.rdd.count() // Also attempt to deserialize as an RDD [SPARK-15791]
    }

    val sparkAnswer =
      try df.collect().toSeq
      catch {
        case e: Exception =>
          val errorMessage =
            s"""
               |Exception thrown while executing query:
               |${df.queryExecution}
               |== Exception ==
               |$e
               |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
          return Some(errorMessage)
      }

    sameRows(expectedAnswer, sparkAnswer, isSorted).map { results =>
      s"""
         |Results do not match for query:
         |Timezone: ${TimeZone.getDefault}
         |Timezone Env: ${sys.env.getOrElse("TZ", "")}
         |
         |${df.queryExecution}
         |== Results ==
         |$results
       """.stripMargin
    }
  }

  def prepareAnswer(answer: Seq[Row], isSorted: Boolean): Seq[Row] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    val converted: Seq[Row] = answer.map(prepareRow)
    if (!isSorted) converted.sortBy(_.toString()) else converted
  }

  // We need to call prepareRow recursively to handle schemas with struct types.
  def prepareRow(row: Row): Row =
    Row.fromSeq(row.toSeq.map {
      case null => null
      case d: java.math.BigDecimal => BigDecimal(d)
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })

  def sameRows(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row],
      isSorted: Boolean = false): Option[String] = {
    val left = prepareAnswer(expectedAnswer, isSorted)
    val right = prepareAnswer(sparkAnswer, isSorted)
    if (left != right) {
      val errorMessage =
        s"""
           |== Results ==
           |${sideBySide(
          s"== Correct Answer - ${expectedAnswer.size} ==" +:
            prepareAnswer(expectedAnswer, isSorted).map(_.toString()),
          s"== Spark Answer - ${sparkAnswer.size} ==" +:
            prepareAnswer(sparkAnswer, isSorted).map(_.toString())).mkString("\n")}
        """.stripMargin
      return Some(errorMessage)
    }
    None
  }

  def checkAnswer(df: DataFrame, expectedAnswer: java.util.List[Row]): String =
    checkAnswer(df, expectedAnswer.asScala) match {
      case Some(errorMessage) => errorMessage
      case None => null
    }

  /**
   * Runs the plan and makes sure the answer is within absTol of the expected result.
   *
   * @param actualAnswer the actual result in a [[Row]].
   * @param expectedAnswer the expected result in a[[Row]].
   * @param absTol the absolute tolerance between actual and expected answers.
   */
  protected def checkAggregatesWithTol(
      actualAnswer: Row,
      expectedAnswer: Row,
      absTol: Double): Unit = {
    require(
      actualAnswer.length == expectedAnswer.length,
      s"actual answer length ${actualAnswer.length} != " +
        s"expected answer length ${expectedAnswer.length}")

    // TODO: support other numeric types besides Double
    // TODO: support struct types?
    actualAnswer.toSeq.zip(expectedAnswer.toSeq).foreach {
      case (actual: Double, expected: Double) =>
        assert(
          math.abs(actual - expected) < absTol,
          s"actual answer $actual not within $absTol of correct answer $expected")
      case (actual, expected) =>
        assert(actual == expected, s"$actual did not equal $expected")
    }
  }
}
