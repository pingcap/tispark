/*
 * Copyright 2023 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.v2

import com.pingcap.tispark.TiTableReference
import org.apache.spark.sql.connector.read.{Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.types.StructType

import java.util.OptionalLong

case class TiDBTableScan(tableRef: TiTableReference, schema: StructType)
    extends Scan
    with SupportsReportStatistics {

  override def readSchema(): StructType = schema

  override def estimateStatistics(): Statistics = {
    new Statistics {
      override def sizeInBytes(): OptionalLong = {
        val sizeInBytes = tableRef.sizeInBytes
        OptionalLong.of(sizeInBytes)
      }

      override def numRows(): OptionalLong = {
        val numRows = tableRef.numRows
        OptionalLong.of(numRows)
      }
    }
  }
}
