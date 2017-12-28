package com.pingcap.tikv.predicates;

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.*;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.predicates.AccessConditionNormalizer.NormalizedCondition;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.Types;
import org.junit.Test;

public class AccessConditionNormalizerTest {
  private static TiTableInfo createTable() {
    return new MetaUtils.TableBuilder()
        .name("testTable")
        .addColumn("c1", DataTypeFactory.of(Types.TYPE_LONG), true)
        .addColumn("c2", DataTypeFactory.of(Types.TYPE_STRING))
        .addColumn("c3", DataTypeFactory.of(Types.TYPE_STRING))
        .addColumn("c4", DataTypeFactory.of(Types.TYPE_TINY))
        .appendIndex("testIndex", ImmutableList.of("c1", "c2"), false)
        .build();
  }

  @Test
  public void normalize() throws Exception {
    TiTableInfo table = createTable();
    // index col = c1, long
    TiExpr cond = new Equal(TiColumnRef.create("c1", table), TiConstant.create(1));
    NormalizedCondition normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof Equal);

    cond = new LessEqual(TiConstant.create(1), TiColumnRef.create("c1", table));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof GreaterEqual);

    cond = new LessThan(TiConstant.create(1), TiColumnRef.create("c1", table));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof GreaterThan);

    cond = new GreaterEqual(TiConstant.create(1), TiColumnRef.create("c1", table));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof LessEqual);

    cond = new GreaterThan(TiConstant.create(1), TiColumnRef.create("c1", table));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof LessThan);

    cond = new Equal(TiConstant.create(1), TiColumnRef.create("c1", table));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof Equal);

    cond = new NotEqual(TiConstant.create(1), TiColumnRef.create("c1", table));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof NotEqual);

    cond = new LessEqual(TiColumnRef.create("c1", table), TiConstant.create(1));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertTrue(normCond.condition instanceof LessEqual);

    cond = new In(TiColumnRef.create("c1", table), TiConstant.create(1), TiConstant.create(2));
    normCond = AccessConditionNormalizer.normalize(cond);
    assertEquals("c1", normCond.columnRef.getName());
    assertEquals(1, normCond.constantVals.get(0).getValue());
    assertEquals(2, normCond.constantVals.get(1).getValue());
    assertTrue(normCond.condition instanceof In);

    cond = new In(TiConstant.create(1), TiColumnRef.create("c1", table), TiConstant.create(2));
    try {
      AccessConditionNormalizer.normalize(cond);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }

    cond =
        new Equal(
            new Divide(TiColumnRef.create("c1", table), TiConstant.create(1)),
            TiConstant.create(1));
    try {
      AccessConditionNormalizer.normalize(cond);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
  }
}
