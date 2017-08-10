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

package com.pingcap.spark

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import java.util.Properties

import com.typesafe.scalalogging.slf4j.Logger

import scala.collection.JavaConversions._

object Utils {
  val DDLSuffix = ".ddl"
  val DataSuffix = ".data"
  val SQLSuffix = ".sql"

  def TryResource[T](res: T) (closeOp: T => Unit) (taskOp: T => Unit): Unit = {
    try {
      taskOp(res)
    } finally {
      closeOp(res)
    }
  }

  def writeFile(content: String, path: String): Unit = {
    TryResource(new PrintWriter(path)) (_.close()) { _.print(content) }
  }

  def readFile(path: String): List[String] = {
    Files.readAllLines(Paths.get(path)).toList
  }

  def getOrThrow(prop: Properties, key: String): String = {
    val v = prop.getProperty(key)
    if (v == null) {
      throw new IllegalArgumentException(key + " is null")
    } else {
      v
    }
  }

  def time[R](block: => R)(logger: Logger): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    logger.info("Elapsed time: " + (t1 - t0) / 1000.0 / 1000.0 / 1000.0 + "s")
    result
  }

  def isDDLFile(path: String) = path.endsWith(DDLSuffix)
  def isDataFile(path: String) = path.endsWith(DataSuffix)
  def isSQLFile(path: String) = path.endsWith(SQLSuffix)

  def ddlFileName(basePath: String, table: String) = Paths.get(basePath, table + DDLSuffix).toAbsolutePath.toString
  def dataFileName(basePath: String, table: String) = Paths.get(basePath, table + DataSuffix).toAbsolutePath.toString
  def joinPath(basePath: String, paths: String*) = Paths.get(basePath, paths: _*).toAbsolutePath.toString
  def ensurePath(basePath: String, paths: String*) = new File(joinPath(basePath, paths: _*)).mkdirs()
}
