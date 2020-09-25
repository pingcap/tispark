/*
 * Copyright 2018 PingCAP, Inc.
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
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.catalog.TiSessionCatalog
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.TiContext
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}

/**
 * TiCommand is used to inherit from [[org.apache.spark.sql.execution.command.RunnableCommand]]
 *
 * Because we are unable to extend from a case class implementing RunnableCommand, we will have
 * to extend TiCommand from its abstract class directly.
 */
abstract class TiCommand(delegate: Command) extends RunnableCommand {
  val tiContext: TiContext
  def tiCatalog: TiSessionCatalog = tiContext.tiCatalog
  override def output: Seq[Attribute] = delegate.output
  override def children: Seq[LogicalPlan] = delegate.children
}
