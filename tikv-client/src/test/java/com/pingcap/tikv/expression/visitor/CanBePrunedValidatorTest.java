package com.pingcap.tikv.expression.visitor;

import static com.pingcap.tikv.expression.visitor.ExpressionTypeCoercerTest.createTable;

import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.expression.visitor.CanBePrunedValidator.PartPruningContext;
import com.pingcap.tikv.meta.TiTableInfo;
import org.junit.Assert;
import org.junit.Test;

public class CanBePrunedValidatorTest {

  @Test
  public void canBePrunedOrWithUnrelatedColCase() {
    TiTableInfo tableInfo = createTable();
    Expression partExpr = ColumnRef.create("c1", tableInfo);
    Expression ts = ColumnRef.create("c3", tableInfo);
    ComparisonBinaryExpression.lessEqual(partExpr, Constant.create(6));
    LogicalBinaryExpression and =
        LogicalBinaryExpression.and(
            ComparisonBinaryExpression.lessEqual(partExpr, Constant.create(6)),
            ComparisonBinaryExpression.greaterThan(partExpr, Constant.create(2)));
    // filter condition is 2 < c1 <= 6 || c3 > 1
    // canBePruned should return false.
    Expression filter =
        LogicalBinaryExpression.or(
            and, ComparisonBinaryExpression.greaterThan(ts, Constant.create(1)));
    Assert.assertFalse(CanBePrunedValidator.canBePruned(filter, new PartPruningContext(partExpr)));
  }

  @Test
  public void canBePrunedNormalCase() {
    TiTableInfo tableInfo = createTable();
    Expression partExpr = ColumnRef.create("c1", tableInfo);
    Expression ts = ColumnRef.create("c3", tableInfo);
    ComparisonBinaryExpression.lessEqual(partExpr, Constant.create(6));

    // filter condition is 2 < c1 <= 6 where c1 is partition expr column.
    // canBePruned should return true.
    LogicalBinaryExpression and =
        LogicalBinaryExpression.and(
            ComparisonBinaryExpression.lessEqual(partExpr, Constant.create(6)),
            ComparisonBinaryExpression.greaterThan(partExpr, Constant.create(2)));
    Assert.assertTrue(CanBePrunedValidator.canBePruned(and, new PartPruningContext(partExpr)));

    // filter condition is 2 < c1 <= 6 && c3 > 1 where c1 is partition expr column.
    // canBePruned should return true.
    LogicalBinaryExpression filter =
        LogicalBinaryExpression.and(
            and, ComparisonBinaryExpression.greaterThan(ts, Constant.create(1)));
    Assert.assertTrue(CanBePrunedValidator.canBePruned(filter, new PartPruningContext(partExpr)));
  }

  @Test
  public void canBePrunedNotPartExprCol() {
    TiTableInfo tableInfo = createTable();
    Expression partExpr = ColumnRef.create("c1", tableInfo);
    Expression ts = ColumnRef.create("c3", tableInfo);
    ComparisonBinaryExpression.lessEqual(ts, Constant.create(6));

    // filter condition is 2 < c1 <= 6 where c1 is partition expr column.
    // canBePruned should return true.
    LogicalBinaryExpression and =
        LogicalBinaryExpression.and(
            ComparisonBinaryExpression.lessEqual(ts, Constant.create(6)),
            ComparisonBinaryExpression.greaterThan(ts, Constant.create(2)));
    Assert.assertFalse(CanBePrunedValidator.canBePruned(and, new PartPruningContext(partExpr)));
  }
}
