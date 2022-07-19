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

package com.pingcap.tikv.expression.visitor;

import static com.pingcap.tikv.expression.FuncCallExpr.Type.YEAR;

import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.ComparisonBinaryExpression.Operator;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.FuncCallExpr;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.expression.LogicalBinaryExpression.Type;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.partition.PartitionedTable.PartitionLocatorContext;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DateType;
import com.pingcap.tikv.types.IntegerType;
import java.sql.Date;
import java.sql.Timestamp;

public class PartitionLocator extends DefaultVisitor<Boolean, PartitionLocatorContext> {

  /**
   * For ComparisonBinaryExpression such as <br>
   * year(birthday@DATE) GREATER_EQUAL 1995, <br>
   * we need to evaluate the result of the left node and compare it with the right node.
   */
  @Override
  public Boolean visit(ComparisonBinaryExpression node, PartitionLocatorContext context) {
    Object data;
    Row row = context.getRow();
    TiTableInfo tableInfo = context.getTableInfo();
    Expression left = node.getLeft();
    if (left instanceof ColumnRef) {
      ColumnRef columnRef = (ColumnRef) left;
      columnRef.resolve(tableInfo);
      data = row.get(columnRef.getColumnOffset(), columnRef.getDataType());
    } else if (left instanceof FuncCallExpr) {
      // TODO: support more function partition
      FuncCallExpr partitionFuncExpr = (FuncCallExpr) left;
      if (partitionFuncExpr.getFuncTp() == YEAR) {
        data = partitionFuncExpr.eval(Constant.create(row.getDate(0), DateType.DATE)).getValue();
      } else {
        throw new UnsupportedOperationException("Partition write only support YEAR() function");
      }
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported expr in range partition %s", left));
    }

    if (!(node.getRight() instanceof Constant)) {
      throw new UnsupportedOperationException(
          String.format("Unsupported right node in partition range expressions %s", node));
    }

    Constant constant = (Constant) node.getRight();
    // For the range with single quote such as varchar 'AAAAA' or date'1995-01-01',
    // we should escape single quote to get the real string need to be compared.
    String boundString = constant.getValue().toString();
    if (data instanceof String || data instanceof Date || data instanceof Timestamp) {
      if (boundString.startsWith("'") && boundString.endsWith("'")) {
        boundString = boundString.substring(1, boundString.length() - 1);
      }
    }
    Operator comparisonType = node.getComparisonType();

    return evaluateComparison(data, boundString, comparisonType);
  }

  Boolean evaluateComparison(Object data, String boundString, Operator comparisonType) {
    // MYSQL IntegerType, we can convert to long and then compare.
    if (data instanceof Number) {
      long dataLongValue = ((Number) data).longValue();
      ;
      long bound = Long.parseLong(boundString);
      switch (comparisonType) {
        case GREATER_EQUAL:
          return dataLongValue >= bound;
        case LESS_THAN:
          return dataLongValue < bound;
        default:
          throw new UnsupportedOperationException("Unsupported comparison type: " + comparisonType);
      }
    } else if (data instanceof String) {
      String dataStringValue = (String) data;
      switch (comparisonType) {
        case GREATER_EQUAL:
          return dataStringValue.compareTo(boundString) >= 0;
        case LESS_THAN:
          return dataStringValue.compareTo(boundString) < 0;
        default:
          throw new UnsupportedOperationException("Unsupported comparison type: " + comparisonType);
      }
    } else if (data instanceof byte[]) {
      String dataStringValue = new String((byte[]) data);
      switch (comparisonType) {
        case GREATER_EQUAL:
          return dataStringValue.compareTo(boundString) >= 0;
        case LESS_THAN:
          return dataStringValue.compareTo(boundString) < 0;
        default:
          throw new UnsupportedOperationException("Unsupported comparison type: " + comparisonType);
      }
    } else if (data instanceof Date) {
      Date dataDateValue = (Date) data;
      Date boundDate = Date.valueOf(boundString);
      switch (comparisonType) {
        case GREATER_EQUAL:
          return dataDateValue.compareTo(boundDate) >= 0;
        case LESS_THAN:
          return dataDateValue.compareTo(boundDate) < 0;
        default:
          throw new UnsupportedOperationException("Unsupported comparison type: " + comparisonType);
      }
    } else if (data instanceof Timestamp) {
      // MySQLType.TypeDatetime is mapping to Spark/TimestampType.
      Timestamp dataTimeValue = (Timestamp) data;
      Timestamp boundDateTime = Timestamp.valueOf(boundString);
      switch (comparisonType) {
        case GREATER_EQUAL:
          return dataTimeValue.compareTo(boundDateTime) >= 0;
        case LESS_THAN:
          return dataTimeValue.compareTo(boundDateTime) < 0;
        default:
          throw new UnsupportedOperationException("Unsupported comparison type: " + comparisonType);
      }
    } else {
      throw new UnsupportedOperationException(
          "Unsupported data type with partition column" + data.getClass());
    }
  }

  /**
   * For partition using MAXVALUE such as "partition p2 values less than MAXVALUE" <br>
   * it will be converted to <br>
   * [[year(birthday@DATE) GREATER_EQUAL ${lower_bound}] AND 1], <br>
   * 1 is Constant standing for always true.
   */
  @Override
  public Boolean visit(Constant node, PartitionLocatorContext context) {
    if (IntegerType.TINYINT.equals(node.getDataType())) {
      return (int) node.getValue() == 1;
    } else {
      throw new IllegalStateException(
          String.format(
              ("Unsupported constant, type: %s, value: %s\n"),
              node.getDataType(),
              node.getValue()));
    }
  }

  /**
   * For logicalBinaryExpression such as [[year(birthday@DATE) GREATER_EQUAL 1995] AND
   * [year(birthday@DATE) LESS_THAN 1997]] we need to get the result of these two
   * ComparisonBinaryExpression.
   */
  @Override
  public Boolean visit(LogicalBinaryExpression node, PartitionLocatorContext context) {
    Expression left = node.getLeft();
    Expression right = node.getRight();

    if (node.getCompType() == Type.AND) {
      return left.accept(this, context) && right.accept(this, context);
    } else {
      throw new UnsupportedOperationException("Unsupported logical binary expression: " + node);
    }
  }
}
