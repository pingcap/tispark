/*
 *
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
 *
 */

package com.pingcap.tispark.statistics.estimate

import com.pingcap.tikv.meta.TiTableInfo
import com.pingcap.tispark.statistics.StatisticsManager

trait TableSizeEstimator {

  /**
   * Returns the estimated size of a row.
   *
   * @param table table to evaluate
   * @return estimated row size in bytes
   */
  def estimatedRowSize(table: TiTableInfo): Long

  /**
   * Returns the estimated number of rows of table.
   * @param table table to evaluate
   * @return estimated rows ins this table
   */
  def estimatedCount(table: TiTableInfo): Long

  /**
   * Returns the estimated size of the table in bytes.
   * @param table table to evaluate
   * @return estimated table size of this table
   */
  def estimatedTableSize(table: TiTableInfo): Long
}

/**
 * An static estimator to estimate row size in bytes.
 * Refer to
 * https://pingcap.com/docs/sql/datatype/#tidb-data-type
 * and
 * https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
 */
object DefaultTableSizeEstimator extends TableSizeEstimator {

  /**
   * Returns the estimated size of the table in bytes.
   * Result overrides [[org.apache.spark.sql.sources.BaseRelation.sizeInBytes]],
   * which decides whether to broadcast the table (by default not to broadcast).
   *
   * @param table table to evaluate
   * @return estimated table size of this table
   */
  override def estimatedTableSize(table: TiTableInfo): Long = {
    val tblCount = estimatedCount(table)
    if (tblCount == Long.MaxValue) {
      Long.MaxValue
    } else {
      val colWidth = estimatedRowSize(table)
      if (Long.MaxValue / colWidth > tblCount) {
        colWidth * tblCount
      } else {
        Long.MaxValue
      }
    }
  }

  /**
   * Returns Pseudo Table Size calculated roughly.
   */
  override def estimatedRowSize(table: TiTableInfo): Long = {
    table.getEstimatedRowSizeInByte
  }

  /**
   * Returns the estimated number of rows of table.
   *
   * @param table table to evaluate
   * @return estimated rows ins this table
   */
  override def estimatedCount(table: TiTableInfo): Long = {
    val tblStats = StatisticsManager.getTableStatistics(table.getId)
    // When getCount is 0, it is possible that statistics information is not read correctly.
    // Just set its estimate count to `Long.Max` since we do not want to broadcast this table.
    if (tblStats != null && tblStats.getCount != 0) {
      tblStats.getCount
    } else {
      Long.MaxValue
    }
  }
}
