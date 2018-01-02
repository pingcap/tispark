package com.pingcap.tikv.predicates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.scalar.Divide;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.expression.scalar.GreaterEqual;
import com.pingcap.tikv.expression.scalar.GreaterThan;
import com.pingcap.tikv.expression.scalar.In;
import com.pingcap.tikv.expression.scalar.LessEqual;
import com.pingcap.tikv.expression.scalar.LessThan;
import com.pingcap.tikv.expression.scalar.NotEqual;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.predicates.AccessConditionNormalizer.NormalizedCondition;
import com.pingcap.tikv.types.StringType;
import com.pingcap.tikv.types.IntegerType;
import org.junit.Test;

public class AccessConditionNormalizerTest {
  private static TiTableInfo createTable() {
    return new MetaUtils.TableBuilder()
        .name("testTable")
        .addColumn("c1", IntegerType.INT, true)
        .addColumn("c2", StringType.VARCHAR)
        .addColumn("c3", StringType.VARCHAR)
        .addColumn("c4", IntegerType.INT)
        .appendIndex("testIndex", ImmutableList.of("c1", "c2"), false)
        .build();
  }

  @Test
  public void normalize() throws Exception {
    TiTableInfo table = createTable();
    // index col = c1, long
    Expression cond = new Equal(ColumnRef.create("c1", table), Constant.create(1));
    NormalizedCondition normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof Equal);

    cond = new LessEqual(Constant.create(1), ColumnRef.create("c1", table));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof GreaterEqual);

    cond = new LessThan(Constant.create(1), ColumnRef.create("c1", table));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof GreaterThan);

    cond = new GreaterEqual(Constant.create(1), ColumnRef.create("c1", table));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof LessEqual);

    cond = new GreaterThan(Constant.create(1), ColumnRef.create("c1", table));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof LessThan);

    cond = new Equal(Constant.create(1), ColumnRef.create("c1", table));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof Equal);

    cond = new NotEqual(Constant.create(1), ColumnRef.create("c1", table));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof NotEqual);

    cond = new LessEqual(ColumnRef.create("c1", table), Constant.create(1));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof LessEqual);

    cond = new In(ColumnRef.create("c1", table), Constant.create(1), Constant.create(2));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertEquals(2, normCond.constantVals.get(1).getValue());
    assertTrue(normCond.condition instanceof In);

    cond = new In(Constant.create(1), ColumnRef.create("c1", table), Constant.create(2));
    try {
      AccessConditionNormalizer.normalize(cond);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }

    cond =
        new Equal(
            new Divide(ColumnRef.create("c1", table), Constant.create(1)),
            Constant.create(1));
    try {
      AccessConditionNormalizer.normalize(cond);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
  }
}
