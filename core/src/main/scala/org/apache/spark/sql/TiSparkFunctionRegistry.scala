package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo, Microsecond, RuntimeReplaceable, TimeToSec, ToDays, ToSeconds, WeekDay, YearWeek}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

// CHECK Spark[[org.apache.spark.sql.catalyst.analysis.FunctionRegistry]]
object TiSparkFunctionRegistry {

// Note: Whenever we add a new entry here, make sure we also update ExpressionToSQLSuite
  val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    // misc non-aggregate functions
    expression[Microsecond]("microsecond"),
    expression[WeekDay]("weekday"),
    expression[ToSeconds]("to_seconds"),
    expression[ToDays]("to_days"),
    expression[TimeToSec]("time_to_sec"),
    expression[YearWeek]("yearweek")
    // TODO: this need import antler 4 into our code base.
    // the behavior is like the following:
    // select extract year from '2019-01-10' which is just
    // identical to select year('2019-01-10')
    // expression[Extract]("extract")
  )

  /** See usage above. */
  private def expression[T <: Expression](
    name: String
  )(implicit tag: ClassTag[T]): (String, (ExpressionInfo, FunctionBuilder)) = {

    // For `RuntimeReplaceable`, skip the constructor with most arguments, which is the main
    // constructor and contains non-parameter `child` and should not be used as function builder.
    val constructors = if (classOf[RuntimeReplaceable].isAssignableFrom(tag.runtimeClass)) {
      val all = tag.runtimeClass.getConstructors
      val maxNumArgs = all.map(_.getParameterCount).max
      all.filterNot(_.getParameterCount == maxNumArgs)
    } else {
      tag.runtimeClass.getConstructors
    }
    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor = constructors.find(_.getParameterTypes.toSeq == Seq(classOf[Seq[_]]))
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        Try(varargCtor.get.newInstance(expressions).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) =>
            // the exception is an invocation exception. To get a meaningful message, we need the
            // cause.
            throw new AnalysisException(e.getCause.getMessage)
        }
      } else {
        // Otherwise, find a constructor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f = constructors.find(_.getParameterTypes.toSeq == params).getOrElse {
          val validParametersCount = constructors
            .filter(_.getParameterTypes.forall(_ == classOf[Expression]))
            .map(_.getParameterCount)
            .distinct
            .sorted
          val expectedNumberOfParameters = if (validParametersCount.length == 1) {
            validParametersCount.head.toString
          } else {
            validParametersCount.init.mkString("one of ", ", ", " and ") +
              validParametersCount.last
          }
          throw new AnalysisException(
            s"Invalid number of arguments for function $name. " +
              s"Expected: $expectedNumberOfParameters; Found: ${params.length}"
          )
        }
        Try(f.newInstance(expressions: _*).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) =>
            // the exception is an invocation exception. To get a meaningful message, we need the
            // cause.
            throw new AnalysisException(e.getCause.getMessage)
        }
      }
    }

    (name, (expressionInfo[T](name), builder))
  }

  /**
   * Creates an [[ExpressionInfo]] for the function as defined by expression T using the given name.
   */
  private def expressionInfo[T <: Expression: ClassTag](name: String): ExpressionInfo = {
    val clazz = scala.reflect.classTag[T].runtimeClass
    val df = clazz.getAnnotation(classOf[ExpressionDescription])
    if (df != null) {
      if (df.extended().isEmpty) {
        new ExpressionInfo(
          clazz.getCanonicalName,
          null,
          name,
          df.usage(),
          df.arguments(),
          df.examples(),
          df.note(),
          df.since()
        )
      } else {
        // This exists for the backward compatibility with old `ExpressionDescription`s defining
        // the extended description in `extended()`.
        new ExpressionInfo(clazz.getCanonicalName, null, name, df.usage(), df.extended())
      }
    } else {
      new ExpressionInfo(clazz.getCanonicalName, name)
    }
  }

}
