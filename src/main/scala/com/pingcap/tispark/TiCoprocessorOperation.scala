package org.apache.spark.sql.catalyst.plans

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DecimalType, DoubleType}
import org.apache.spark.sql.util.CollectionUtils.RichIterable

/**
  * Note: Taken from SAP HANA-VORA extensions, they've done excellent job already
  *
  * This partial aggregation matches Tungsten processed aggregates. We distinguish between final and
  * partial aggregation properties. A partial aggregation is always in two phases: the results of
  * the partial aggregation is the input to the final one.
  *
  * The returned values for this match are as follows
  * (see [[PartialAggregation.ReturnType]]):
  *  - Grouping expressions for the final aggregation (finalGroupingExpressions)
  *  - Aggregate expressions for the final aggregation (finalAggregateExpressions)
  *  - Grouping expressions for the partial aggregation. (partialGroupingExpressions)
  *  - Aggregate expressions for the partial aggregation. (partialAggregateExpressionsForPushdown)
  *  - Mapping of the aggregate functions to the
  *         appropriate attributes (aggregateFunctionToAttribute)
  *  - the rewritten result expressions of the partial aggregation
  *  - remaining logical plan (child)
  *
  *
  */
object PartialAggregation extends Logging {

  /**
    * Determine if the given function is eligible for partial aggregation.
    *
    * @param aggregateFunction The aggregate function.
    * @return `true` if the given aggregate function is not supported for partial aggregation,
    * `false` otherwise.
    */
  def nonSupportedAggregateFunction(aggregateFunction: AggregateFunction): Boolean =
    aggregateFunction match {
      case _: Count => false
      case _: Sum => false
      case _: Min => false
      case _: Max => false
      case _: Average => false
      case _ => true
    }

  type ReturnType = (Seq[NamedExpression], Seq[AggregateExpression], Seq[NamedExpression],
    Seq[NamedExpression], Map[(AggregateFunction, Boolean), Attribute],
    Seq[NamedExpression], LogicalPlan)

  // scalastyle:off cyclomatic.complexity
  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {

    // Make sure that only our handled partial aggregates are processed.
    case logical.Aggregate(_, resultExpressions, _) if resultExpressions.flatMap(
      expr => expr.collect { case agg: AggregateExpression => agg })
      .exists(agg => nonSupportedAggregateFunction(agg.aggregateFunction)) =>
      logWarning("Found an aggregate function that could not be pushed down - falling back " +
        "to normal behavior")
      None

    // There could be a possible parser Bug similar to the one detected in
    // [[SparkStrategies]] - see this class for further reference
    case logical.Aggregate(_, resultExpressions, _) if resultExpressions.flatMap(
      expr => expr.collect { case agg: AggregateExpression => agg })
      .filter(_.isDistinct).map(_.aggregateFunction.children).distinct.length > 1 =>
      // This is a sanity check. We should not reach here when we have multiple distinct
      // column sets. [[org.apache.spark.sql.catalyst.analysis.DistinctAggregationRewriter]]
      // should take care of this case.
      sys.error("You hit a query analyzer bug. Please report your query to " +
        "Spark user mailing list. It is the same bug as reported in Spark Strategies, multiple " +
        "distinct column sets should be resolved and planned differently.")

    // Assuming that resultExpressions is empty, this would mean that this function basically
    // returns a plan with no result, thus we need to fix this
    // this case applies to DISTINCTS that are rewritten as an aggregate
    case logical.Aggregate(groupingExpressions, resultExpressions, child)
      if resultExpressions.isEmpty =>
      val actualResultExpressions = groupingExpressions.collect{
        case ne: NamedExpression => ne
      }
      Some(planDistributedAggregateExecution(groupingExpressions, actualResultExpressions, child))


    // This case tries to create a partial aggregation
    case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
      Some(planDistributedAggregateExecution(groupingExpressions, resultExpressions, child))

    case _ => None
  }
  // scalastyle:on cyclomatic.complexity

  // scalastyle:off cyclomatic.complexity method.length
  private def planDistributedAggregateExecution(groupingExpressions: Seq[Expression],
                                                resultExpressions: Seq[NamedExpression],
                                                child: LogicalPlan): ReturnType = {
    /**
      * A single aggregate expression might appear multiple times in resultExpressions.
      * In order to avoid evaluating an individual aggregate function multiple times, we'll
      * build a seq of the distinct aggregate expressions and build a function which can
      * be used to re-write expressions so that they reference the single copy of the
      * aggregate function which actually gets computed.
      */
    val (nonDistinctAggregateExpressions, aggregateExpressionsToNamedExpressions) =
      resultExpressions
        .foldLeft(
          Seq.empty[AggregateExpression] -> Map.empty[AggregateExpression, Set[NamedExpression]]) {
          case ((aggExpressions, aggExpressionsToNamedExpressions), current) =>
            val aggregateExpressionsInCurrent =
              current.collect { case a: AggregateExpression => a }
            /**
              * We keep track of the outermost [[NamedExpression]] referencing the
              * [[AggregateExpression]] to always have distinct names for the named pushdown
              * expressions.
              */
            val updatedMapping =
              aggregateExpressionsInCurrent.foldLeft(aggExpressionsToNamedExpressions) {
                case (mapping, expression) =>
                  val currentMapping = mapping.getOrElse(expression, Set.empty)
                  mapping.updated(expression, currentMapping + current)
              }
            (aggExpressions ++ aggregateExpressionsInCurrent) -> updatedMapping
        }

    val aggregateExpressions = nonDistinctAggregateExpressions.distinct

    /**
      * We split and rewrite the given aggregate expressions to partial aggregate expressions
      * and keep track of the original aggregate expression for later referencing.
      */
    val aggregateExpressionsToAliases: Map[AggregateExpression, Seq[Alias]] =
      aggregateExpressions.map { agg =>
        agg -> rewriteAggregateExpressionsToPartial(
          agg,
          aggregateExpressionsToNamedExpressions(agg))
      }(collection.breakOut)

    /**
      * Since for pushdown only [[NamedExpression]]s are allowed, we do the following:
      * 1. We extract [[Attribute]]s that can be pushed down straight.
      * 2. For [[Expression]]s with [[AggregateExpression]]s in them, we can only push down the
      *    [[AggregateExpression]]s and leave the [[Expression]] for evaluation on spark side.
      *    If the [[Expression]] does not contain any [[AggregateExpression]], we can also directly
      *    push it down.
      */
    val pushdownExpressions = resultExpressions.flatMap {
      case attr: Attribute => Seq(attr)
      case Alias(attr: Attribute, _) => Seq(attr)
      case alias@Alias(expression, _) =>
        /**
          * If the collected sequence of [[AggregateExpression]]s is empty then there
          * is no dependency of a regular [[Expression]] to a distributed value computed by
          * an [[AggregateExpression]] and as such we can push it down. Otherwise, we just push
          * down the [[AggregateExpression]]s and apply the other [[Expression]]s via the
          * resultExpressions.
          */
        expression.collect {
          case agg: AggregateExpression => agg
        }.nonEmptyOpt.getOrElse(Seq(alias))
      case _ => Seq.empty
    }.distinct

    /**
      * With this step, we replace the pushdownExpressions with the corresponding
      * [[NamedExpression]]s that we can continue to work on. Regular [[NamedExpression]]s are
      * 'replaced' by themselves whereas the [[AggregateExpression]]s are replaced by their
      * partial versions hidden behind [[Alias]]es.
      */
    val namedPushdownExpressions = pushdownExpressions.flatMap {
      case agg: AggregateExpression => aggregateExpressionsToAliases(agg)
      case namedExpression: NamedExpression => Seq(namedExpression)
    }

    // For those distinct aggregate expressions, we create a map from the
    // aggregate function to the corresponding attribute of the function.
    val aggregateFunctionToAttribute = aggregateExpressions.map { agg =>
      val aggregateFunction = agg.aggregateFunction
      val attribute = Alias(aggregateFunction, aggregateFunction.toString)().toAttribute
      (aggregateFunction, agg.isDistinct) -> attribute
    }.toMap

    val namedGroupingExpressions = groupingExpressions.map {
      case ne: NamedExpression => ne -> ne

      /** If the expression is not a NamedExpressions, we add an alias.
        * So, when we generate the result of the operator, the Aggregate Operator
        * can directly get the Seq of attributes representing the grouping expressions.
        */
      case other =>
        val existingAlias = resultExpressions.find({
          case Alias(aliasChild, aliasName) => aliasChild == other
          case _ => false
        })
        // it could be that there is already an alias, so do not "double alias"
        val mappedExpression = existingAlias match {
          case Some(alias) => alias.toAttribute
          case None => Alias(other, other.toString)()
        }
        other -> mappedExpression
    }
    /** This step is separate to keep the input order of the groupingExpressions */
    val groupExpressionMap = namedGroupingExpressions.toMap

    // make expression out of the tuples
    val finalGroupingExpressions = namedGroupingExpressions.map(_._2)
    val finalAggregateExpressions = aggregateExpressions.map(_.copy(mode = Final))

    /** Extracted from the `Aggregation` Strategy of Spark:
      * The original `resultExpressions` are a set of expressions which may reference
      * aggregate expressions, grouping column values, and constants. When aggregate operator
      * emits output rows, we will use `resultExpressions` to generate an output projection
      * which takes the grouping columns and final aggregate result buffer as input.
      * Thus, we must re-write the result expressions so that their attributes match up with
      * the attributes of the final result projection's input row:
      */
    val rewrittenResultExpressions = resultExpressions.map { expr =>
      expr.transformDown {
        case AggregateExpression(aggregateFunction, _, isDistinct, _) =>
          // The final aggregation buffer's attributes will be `finalAggregationAttributes`,
          // so replace each aggregate expression by its corresponding attribute in the set:
          aggregateFunctionToAttribute(aggregateFunction, isDistinct)
        case expression =>

          /**
            * Since we're using `namedGroupingAttributes` to extract the grouping key
            * columns, we need to replace grouping key expressions with their corresponding
            * attributes. We do not rely on the equality check at here since attributes may
            * differ cosmetically. Instead, we use semanticEquals.
            */
          groupExpressionMap.collectFirst {
            case (grpExpr, ne) if grpExpr semanticEquals expression => ne.toAttribute
          }.getOrElse(expression)
      }.asInstanceOf[NamedExpression]
    }

    // this is the [[ReturnType]]
    (finalGroupingExpressions,
      finalAggregateExpressions,
      /* *
       * final and partial grouping expressions are the same! Still needed for Tungsten/
       * SortBasedAggregate Physical plan node
       */
      finalGroupingExpressions,
      namedPushdownExpressions,
      aggregateFunctionToAttribute,
      rewrittenResultExpressions,
      child)
  }
  // scalastyle:on


  /**
    * This method rewrites an [[AggregateExpression]] to the corresponding partial ones.
    * For instance an [[Average]] is rewritten to a [[Sum]] and a [[Count]].
    *
    * @param aggregateExpression [[AggregateExpression]] to rewrite.
    * @return A sequence of [[Alias]]es that represent the split up [[AggregateExpression]].
    */
  private def rewriteAggregateExpressionsToPartial(
                                                    aggregateExpression: AggregateExpression,
                                                    outerNamedExpressions: Set[NamedExpression]): Seq[Alias] = {
    val outerName = outerNamedExpressions.map(_.name).toSeq.sorted.mkString("", "_", "_")
    val inputBuffers = aggregateExpression.aggregateFunction.inputAggBufferAttributes
    aggregateExpression.aggregateFunction match {
      case avg: Average => {
        // two: sum and count
        val Seq(sumAlias, countAlias, _*) = inputBuffers
        val typedChild = avg.child.dataType match {
          case DoubleType | DecimalType.Fixed(_, _) => avg.child
          case _ => Cast(avg.child, DoubleType)
        }
        val sumExpression = // sum
          AggregateExpression(Sum(typedChild), mode = Partial, aggregateExpression.isDistinct)
        val countExpression = // count
          AggregateExpression(Count(avg.child), mode = Partial, aggregateExpression.isDistinct)
        Seq(
          referenceAs(outerName + sumAlias.name, sumAlias, sumExpression),
          referenceAs(outerName + countAlias.name, countAlias, countExpression))
      }
      case Count(_) | Sum(_) | Max(_) | Min(_) =>
        inputBuffers.map(ref =>
          referenceAs(outerName + ref.name, ref, aggregateExpression.copy(mode = Partial)))
      case _ => throw new RuntimeException("Approached rewrite with unsupported expression")
    }

  }

  /**
    * References a given [[Expression]] as an [[Alias]] of the given [[Attribute]].
    *
    * @param attribute The [[Attribute]] to create the [[Alias]] reference of.
    * @param expression The [[Expression]] to reference as the given [[Attribute]].
    * @return An [[Alias]] of the [[Expression]] referenced as the [[Attribute]].
    */
  private def referenceAs(name: String, attribute: Attribute, expression: Expression): Alias = {
    Alias(expression, name)(attribute.exprId, attribute.qualifier, Some(attribute.metadata))
  }
}
