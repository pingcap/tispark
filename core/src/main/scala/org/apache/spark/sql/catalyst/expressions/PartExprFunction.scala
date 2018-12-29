package org.apache.spark.sql.catalyst.expressions

import java.util.Calendar

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

// TODO: (zhexuany), extract need change sqlBase.g4 file since extract need
// support 'extract (fn from date)' syntax. One example of this new syntax
// could be `EXTRACT(DAY_MINUTE FROM '2019-07-02 01:02:03')`. So, let's leave
// it in next PR.

// Returns the time argument, converted to seconds.
case class TimeToSec(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  @transient private lazy val c = Calendar.getInstance(DateTimeUtils.defaultTimeZone())

  override protected def nullSafeEval(date: Any): Any = {
    val ts = date.asInstanceOf[Long]
    c.setTimeInMillis(ts / 1000L)
    val hour = c.get(Calendar.HOUR_OF_DAY)
    val minute = c.get(Calendar.MINUTE)
    val sec = c.get(Calendar.SECOND)
    hour * 60 * 60 + minute * 60 + sec
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(
      ctx,
      ev,
      time => {
        val cal = classOf[Calendar].getName
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        val c = "calWeekDay"
        ctx.addImmutableStateIfNotExists(
          cal,
          c,
          v => s"""$v = $cal.getInstance($dtu.defaultTimeZone());
           """.stripMargin
        )
        s"""
        $c.setTimeInMillis($time / 1000L);
        ${ev.value} = $c.get($cal.HOUR_OF_DAY) * 60 * 60 + $c.get($cal.MINUTE) * 60 + $c.get($c.SECOND);
      """
      }
    )

  override def prettyName: String = "time_to_sec"
}

//  Given a date date, returns a day number (the number of days since year 0).
case class ToDays(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  @transient private lazy val daysFromZero = {
    // since the input is valid, we are no need to guard this
    // the result of this is -719528
    DateTimeUtils.stringToDate(UTF8String.fromString("0000-01-01")).get + 2
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any =
    DateTimeUtils
      .millisToDays(date.asInstanceOf[Long] / 1000L) - daysFromZero

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.millisToDays($c/1000L) + 719528")
  }

  override def prettyName: String = "to_days"
}

// Given a date or datetime expr, returns the number of seconds since the year 0.
// If expr is not a valid date or datetime value, returns NULL.
case class ToSeconds(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  @transient private lazy val c = {
    val c = Calendar.getInstance(DateTimeUtils.defaultTimeZone())
    c
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = LongType

  @transient private lazy val daysFromZero = {
    // since the input is valid, we are no need to guard this
    // the result of this is -719528
    DateTimeUtils.stringToDate(UTF8String.fromString("0000-01-01")).get + 2
  }

  // MySQL only supports date, datetime. Time in the form "23:59:59" is considered as invalid
  // format the expected result is null.
  override protected def nullSafeEval(date: Any): Any = {
    val ts = date.asInstanceOf[Long]
    val hour = DateTimeUtils.getHours(ts)
    val minute = DateTimeUtils.getMinutes(ts)
    val sec = DateTimeUtils.getSeconds(ts)
    hour * 60 * 60 + minute * 60 + sec + (DateTimeUtils
      .millisToDays(ts / 1000L) - daysFromZero) * 24 * 3600L
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(
      ctx,
      ev,
      c =>
        s"$dtu.getHours($c)*60*60" +
          s"+ $dtu.getMinutes($c)*60" +
          s"+ $dtu.getSeconds($c)" +
          s"+ ($dtu.millisToDays($c/1000L) + 719528) * 24 * 3600L"
    )
  }

  override def prettyName: String = "to_seconds"
}

// from the year in the date argument for the first and the last week of the year.
case class YearWeek(timeExp: Expression, mode: Expression)
    extends BinaryExpression
    with ImplicitCastInputTypes {

  def this(time: Expression) = {
    this(time, Literal(0, IntegerType))
  }

  override def left: Expression = timeExp
  override def right: Expression = mode

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = StringType

  @transient private lazy val c = {
    val c = Calendar.getInstance(DateTimeUtils.getTimeZone("UTC"))
    c.setFirstDayOfWeek(Calendar.MONDAY)
    c.setMinimalDaysInFirstWeek(4)
    c
  }

  // YearWeek has identical behavior with Week.
  // https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_week
  private def adjustCalendarByMode(mode: Int): Unit =
    mode match {
      // First day of week is Sunday.
      // Week 1 is the first week with a Sunday in this year.
      // Week range is from 0 to 53.
      case 0 =>
        c.setFirstDayOfWeek(Calendar.SUNDAY)
        c.setMinimalDaysInFirstWeek(7)
      // First day of week is Monday.
      // Week 1 is the first week with 4 or more days this year.
      // Week range is from 0 to 53.
      case 1 =>
        c.setFirstDayOfWeek(Calendar.MONDAY)
        c.setMinimalDaysInFirstWeek(4)
      // First day of week is Sunday.
      // Week 1 is the first week with a Sunday in this year.
      // Week range is from 1 to 53.
      case 2 =>
        c.setFirstDayOfWeek(Calendar.SUNDAY)
        c.setMinimalDaysInFirstWeek(7)
      // First day of week is Monday.
      // Week 1 is the first week with 4 or more days this year.
      // Week range is from 1 to 53.
      case 3 =>
        c.setFirstDayOfWeek(Calendar.MONDAY)
        c.setMinimalDaysInFirstWeek(4)
      // First day of week is Sunday.
      // Week 1 is the first week withSunday 4 or more days this year.
      // Week range is from 0 to 53.
      case 4 =>
        c.setFirstDayOfWeek(Calendar.SUNDAY)
        c.setMinimalDaysInFirstWeek(4)
      // First day of week is Monday.
      // Week 1 is the first week with a Monday in this year.
      // Week range is from 0 to 53.
      case 5 =>
        c.setFirstDayOfWeek(Calendar.MONDAY)
        c.setMinimalDaysInFirstWeek(7)
      // First day of week is Sunday.
      // Week 1 is the first week with 4 or more days this year.
      // Week range is from 1 to 53.
      case 6 =>
        c.setFirstDayOfWeek(Calendar.SUNDAY)
        c.setMinimalDaysInFirstWeek(4)
      // First day of week is Monday.
      // Week 1 is the first week with a Monday in this year.
      // Week range is from 1 to 53.
      case 7 =>
        c.setFirstDayOfWeek(Calendar.MONDAY)
        c.setMinimalDaysInFirstWeek(7)
      // Mysql's behavior is ignoring invalid mode input and falls
      // back to mode 0.
      case _ =>
        c.setFirstDayOfWeek(Calendar.SUNDAY)
        c.setMinimalDaysInFirstWeek(7)
    }

  override protected def nullSafeEval(date: Any, mode: Any): Any = {
    adjustCalendarByMode(mode.asInstanceOf[Int])
    c.setTimeInMillis(date.asInstanceOf[Int] * 1000L * 3600L * 24L)
    c.getWeekYear * 100 + c.get(Calendar.WEEK_OF_YEAR) + ""
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(
      ctx,
      ev,
      (time, m) => {
        val cal = classOf[Calendar].getName
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        val c = "calYearWeek"
        ctx.addImmutableStateIfNotExists(
          cal,
          c,
          v => s"""$v = $cal.getInstance($dtu.getTimeZone("UTC"));"""
        )
        s"""
          switch($m) {
            case 0:
        |    $c.setFirstDayOfWeek($cal.SUNDAY);
        |    $c.setMinimalDaysInFirstWeek(7);
              break;
            case 1:
        |    $c.setFirstDayOfWeek($cal.MONDAY);
        |    $c.setMinimalDaysInFirstWeek(4);
              break;
            case 2:
        |    $c.setFirstDayOfWeek($cal.SUNDAY);
        |    $c.setMinimalDaysInFirstWeek(7);
              break;
            case 3:
        |    $c.setFirstDayOfWeek($cal.MONDAY);
        |    $c.setMinimalDaysInFirstWeek(4);
              break;
            case 4:
        |    $c.setFirstDayOfWeek($cal.SUNDAY);
        |    $c.setMinimalDaysInFirstWeek(4);
              break;
            case 5:
        |    $c.setFirstDayOfWeek($cal.MONDAY);
        |    $c.setMinimalDaysInFirstWeek(7);
              break;
            case 6:
        |    $c.setFirstDayOfWeek($cal.SUNDAY);
        |    $c.setMinimalDaysInFirstWeek(4);
              break;
            case 7:
        |    $c.setFirstDayOfWeek($cal.MONDAY);
        |    $c.setMinimalDaysInFirstWeek(7);
              break;
            default:
        |    $c.setFirstDayOfWeek($cal.SUNDAY);
        |    $c.setMinimalDaysInFirstWeek(7);
          }
          $c.setTimeInMillis($time * 1000L * 3600L * 24L);
          ${ev.value} = UTF8String.fromString($c.getWeekYear() * 100 + $c.get($cal.WEEK_OF_YEAR) + "");
      """
      }
    )

  override def prettyName: String = "yearweeek"
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage =
    "_FUNC_(date) - Returns the weekday index for date(0 = Monday, 1 = Tuesday, … 6 = Sunday).",
  examples = """
    Examples:
      > SELECT _FUNC_('2008-02-03');
       6
  """,
  since = "2.3.2"
)
// scalastyle:on line.size.limit
case class WeekDay(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  @transient private lazy val c = {
    val c = Calendar.getInstance(DateTimeUtils.defaultTimeZone())
    c.setFirstDayOfWeek(Calendar.MONDAY)
    c
  }

  override protected def nullSafeEval(date: Any): Any = {
    c.setTimeInMillis(date.asInstanceOf[Int] * 1000L * 3600L * 24L)
    // Monday is the first day of week and its index is 0 where as its index in Spark is 1.
    // That is why we need subtract 1 from original value.
    c.get(Calendar.DAY_OF_WEEK) - 1
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(
      ctx,
      ev,
      time => {
        val cal = classOf[Calendar].getName
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        val c = "calWeekDay"
        ctx.addImmutableStateIfNotExists(
          cal,
          c,
          v => s"""$v = $cal.getInstance($dtu.defaultTimeZone());
                  |$c.setFirstDayOfWeek($cal.MONDAY);
           """.stripMargin
        )
        s"""
        $c.setTimeInMillis($time * 1000L * 3600L * 24L);
        ${ev.value} = $c.get($cal.DAY_OF_WEEK) - 1;
      """
      }
    )

  override def prettyName: String = "weekday"
}

// Returns the microseconds from the time or datetime expression expr as
// a number in the range from 0 to 999999.
case class Microsecond(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any =
    DateTimeUtils.toJavaTimestamp(date.asInstanceOf[Long]).getNanos / 1000

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.toJavaTimestamp($c).getNanos()/1000")
  }

  override def prettyName: String = "microsecond"
}
