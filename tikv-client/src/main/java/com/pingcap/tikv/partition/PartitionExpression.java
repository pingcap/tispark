/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.partition;

import com.pingcap.tikv.expression.Expression;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PartitionExpression implements Serializable {

  // the original partition expression from TiTableInfo,
  // it can be a column or a function when Hash partition and ranging partition
  private Expression originExpression;

  // key is the partition column name, value is ranges' expressions of the partition column
  private Map<String, List<Expression>> rangeColumnRefBoundExpressions;

  // the ranges' expressions of the original partition expression when ranging partition.
  private List<Expression> rangePartitionBoundExpressions;
}
