/*
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
 */

package org.apache.spark.sql.catalyst.expressions.aggregate

import gnu.trove.list.linked.TLongLinkedList
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

case class CollectHandles(child: Expression,
                          mutableAggBufferOffset: Int = 0,
                          inputAggBufferOffset: Int = 0)
    extends ImperativeAggregate {
  def this(child: Expression) = this(child, 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "CollectHandles"

  // we know handles are long type data, so using a type-specified linked list as buffer
  private val buffer: TLongLinkedList = new TLongLinkedList(512)

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(child.dataType)

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def supportsPartial: Boolean = false

  override def aggBufferAttributes: Seq[AttributeReference] = Nil

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override def inputAggBufferAttributes: Seq[AttributeReference] = Nil

  // `CollectHandles` is non-deterministic since it's result depends on the
  // actual order of input rows.
  override def deterministic: Boolean = false

  override def initialize(b: InternalRow): Unit = {
    buffer.clear()
  }

  override def update(b: InternalRow, input: InternalRow): Unit = {
    val value = child.eval(input)
    if (value != null) {
      buffer.add(value.asInstanceOf[Long])
    }
  }

  override def merge(buffer: InternalRow, input: InternalRow): Unit = {
    sys.error("Collect cannot be used in partial aggregations.")
  }

  override def eval(input: InternalRow): Any = {
    new GenericArrayData(buffer.toArray)
  }
}
