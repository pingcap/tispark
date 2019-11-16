package org.apache.spark.sql.execution

import com.pingcap.tikv.columnar.{TiColumnVector, TiColumnVectorAdapter, TiColumnarBatch}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.DataType

/**
 * Helper trait for abstracting scan functionality using
 * [[TiColumnarBatch]]es.
 */
trait TiColumnarBatchScan extends CodegenSupport {

  def vectorTypes: Option[Seq[String]] = None

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time")
  )

  /**
   * Generate [[TiColumnVector]] expressions for our parent to consume as rows.
   * This is called once per [[TiColumnarBatch]].
   */
  private def genCodeColumnVector(ctx: CodegenContext,
                                  columnVar: String,
                                  ordinal: String,
                                  dataType: DataType,
                                  nullable: Boolean): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val value = ctx.getValue(columnVar, dataType, ordinal)
    val isNullVar = if (nullable) { ctx.freshName("isNull") } else { "false" }
    val valueVar = ctx.freshName("value")
    val str = s"columnVector[$columnVar, $ordinal, ${dataType.simpleString}]"
    val code = s"${ctx.registerComment(str)}\n" + (if (nullable) {
                                                     s"""
        boolean $isNullVar = $columnVar.isNullAt($ordinal);
        $javaType $valueVar = $isNullVar ? ${ctx.defaultValue(dataType)} : ($value);
      """
                                                   } else {
                                                     s"$javaType $valueVar = $value;"
                                                   }).trim
    ExprCode(code, isNullVar, valueVar)
  }

  /**
   * Produce code to process the input iterator as [[TiColumnarBatch]]es.
   * This produces an [[UnsafeRow]] for each row in each batch.
   */
  override protected def doProduce(ctx: CodegenContext): String = {
    // PhysicalRDD always just has one input
    val input = ctx.addMutableState("scala.collection.Iterator", "input", v => s"$v = inputs[0];")

    // metrics
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val scanTimeMetric = metricTerm(ctx, "scanTime")
    val scanTimeTotalNs = ctx.addMutableState(ctx.JAVA_LONG, "scanTime") // init as scanTime = 0

    val tiBatchClz = classOf[TiColumnarBatch].getName
    val batch = ctx.addMutableState(tiBatchClz, "batch")

    val idx = ctx.addMutableState(ctx.JAVA_INT, "batchIdx")
    val columnVectorClz = classOf[TiColumnVectorAdapter].getName
    val (colVars, columnAssigns) = output.indices.map {
      case i =>
        val name = ctx.addMutableState(columnVectorClz, s"colInstance$i")
        (name, s"$name = ($columnVectorClz) $batch.column($i);")
    }.unzip

    val nextBatch = ctx.freshName("nextBatch")
    val nextBatchFuncName = ctx.addNewFunction(
      nextBatch,
      s"""
         |private void $nextBatch() throws java.io.IOException {
         |  long getBatchStart = System.nanoTime();
         |  if ($input.hasNext()) {
         |    $batch = ($tiBatchClz)$input.next();
         |    $numOutputRows.add($batch.numRows());
         |    $idx = 0;
         |    ${columnAssigns.mkString("", "\n", "\n")}
         |  }
         |  $scanTimeTotalNs += System.nanoTime() - getBatchStart;
         |}""".stripMargin
    )

    ctx.currentVars = null
    val rowIdx = ctx.freshName("rowIdx")
    val columnsBatchInput = (output zip colVars).map {
      case (attr, colVar) =>
        genCodeColumnVector(ctx, colVar, rowIdx, attr.dataType, attr.nullable)
    }
    val localIdx = ctx.freshName("localIdx")
    val localEnd = ctx.freshName("localEnd")
    val numRows = ctx.freshName("numRows")
    val shouldStop = if (parent.needStopCheck) {
      s"if (shouldStop()) { $idx = $rowIdx + 1; return; }"
    } else {
      "// shouldStop check is eliminated"
    }

    s"""
       |if ($batch == null) {
       |  $nextBatchFuncName();
       |}
       |while ($batch != null) {
       |  int $numRows = $batch.numRows();
       |  int $localEnd = $numRows - $idx;
       |  for (int $localIdx = 0; $localIdx < $localEnd; $localIdx++) {
       |    int $rowIdx = $idx + $localIdx;
       |    ${consume(ctx, columnsBatchInput).trim}
       |    $shouldStop
       |  }
       |  $idx = $numRows;
       |  $batch = null;
       |  $nextBatchFuncName();
       |}
       |$scanTimeMetric.add($scanTimeTotalNs / (1000 * 1000));
       |$scanTimeTotalNs = 0;
     """.stripMargin
  }
}
