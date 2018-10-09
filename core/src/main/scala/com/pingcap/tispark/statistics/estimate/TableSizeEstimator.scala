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
import com.pingcap.tikv.types.MySQLType._
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
   * Returns Pseudo Table Size calculated roughly.
   */
  override def estimatedRowSize(table: TiTableInfo): Long = {
    // Magic number used for estimating table size
    val goldenSplitFactor = 0.618
    val complementFactor = 1 - goldenSplitFactor

    import scala.collection.JavaConversions._
    table.getColumns
      .map(_.getType.getType)
      .map {
        case TypeTiny       => 1
        case TypeShort      => 2
        case TypeInt24      => 3
        case TypeLong       => 4
        case TypeLonglong   => 8
        case TypeFloat      => 4
        case TypeDouble     => 8
        case TypeDecimal    => 10
        case TypeNewDecimal => 10
        case TypeNull       => 1
        case TypeTimestamp  => 4
        case TypeDate       => 3
        case TypeYear       => 1
        case TypeDatetime   => 8
        case TypeDuration   => 3
        case TypeString     => 255 * goldenSplitFactor
        case TypeVarchar    => 255 * goldenSplitFactor
        case TypeVarString  => 255 * goldenSplitFactor
        case TypeTinyBlob   => 1 << (8 * goldenSplitFactor).toInt
        case TypeBlob       => 1 << (16 * goldenSplitFactor).toInt
        case TypeMediumBlob => 1 << (24 * goldenSplitFactor).toInt
        case TypeLongBlob   => 1 << (32 * goldenSplitFactor).toInt
        case TypeEnum       => 2
        case TypeSet        => 8
        case TypeBit        => 8 * goldenSplitFactor
        case TypeJSON       => 1 << (10 * goldenSplitFactor).toInt
        case _ =>
          complementFactor * Int.MaxValue // for other types we just estimate as complementFactor * Int.MaxValue
      }
      .sum
      .toLong
  }

  /**
   * Returns the estimated number of rows of table.
   *
   * @param table table to evaluate
   * @return estimated rows ins this table
   */
  override def estimatedCount(table: TiTableInfo): Long = {
    val tblStats = StatisticsManager.getTableStatistics(table.getId)
    if (tblStats != null) {
      tblStats.getCount
    } else {
      Long.MaxValue
    }
  }

  /**
   * Returns the estimated size of the table in bytes.
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
}
