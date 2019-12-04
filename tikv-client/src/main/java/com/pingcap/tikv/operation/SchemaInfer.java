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

package com.pingcap.tikv.operation;

import com.pingcap.tikv.expression.ByItem;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.types.DataType;
import java.util.ArrayList;
import java.util.List;

/**
 * SchemaInfer extract row's type after query is executed. It is pretty rough version. Optimization
 * is on the way. The problem we have right now is that TiDB promote Sum to Decimal which is not
 * compatible with column's type. The solution we come up with right now is use record column's type
 * ad finalFieldType and build another list recording TiExpr's type as fieldType for row reading.
 * Once we finish row reading, we first check each element in fieldType and finalFieldType share the
 * same type or not. If yes, no need for casting. If no, casting is needed here.
 */
public class SchemaInfer {
  private List<DataType> types;

  public static SchemaInfer create(TiDAGRequest dagRequest) {
    return create(dagRequest, false);
  }

  public static SchemaInfer create(TiDAGRequest dagRequest, boolean readHandle) {
    return new SchemaInfer(dagRequest.copy(), readHandle);
  }

  private SchemaInfer(TiDAGRequest dagRequest, boolean readHandle) {
    types = new ArrayList<>();
    dagRequest.init(readHandle);
    extractFieldTypes(dagRequest, readHandle);
  }

  /**
   * TODO: order by extract field types from tiSelectRequest for reading data to row.
   *
   * @param dagRequest is SelectRequest
   */
  private void extractFieldTypes(TiDAGRequest dagRequest, boolean readHandle) {
    if (readHandle) {
      // or extract data from index read
      types.addAll(dagRequest.getIndexDataTypes());
    } else if (dagRequest.hasPushDownAggregate()) {
      dagRequest
          .getPushDownAggregates()
          .forEach(expr -> types.add(dagRequest.getExpressionType(expr)));
      // In DAG mode, if there is any group by statement in a request, all the columns specified
      // in group by expression will be returned, so when we decode a result row, we need to pay
      // extra attention to decoding.
      if (dagRequest.hasPushDownGroupBy()) {
        for (ByItem item : dagRequest.getPushDownGroupBys()) {
          types.add(dagRequest.getExpressionType(item.getExpr()));
        }
      }
    } else {
      // Extract all column type information from TiExpr
      dagRequest.getFields().forEach(expr -> types.add(expr.getType()));
    }
  }

  public DataType getType(int index) {
    return types.get(index);
  }

  public List<DataType> getTypes() {
    return types;
  }
}
