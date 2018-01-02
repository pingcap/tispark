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

package com.pingcap.tikv.predicates;

import static com.pingcap.tikv.expression.LogicalBinaryExpression.and;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.types.IntegerType;
import java.util.List;
import org.junit.Test;

public class PredicateUtilsTest {
  @Test
  public void mergeCNFExpressionsTest() throws Exception {
    Constant c1 = Constant.create(1, IntegerType.INT);
    Constant c2 = Constant.create(2, IntegerType.INT);
    Constant c3 = Constant.create(3, IntegerType.INT);
    Constant c4 = Constant.create(4, IntegerType.INT);
    Constant c5 = Constant.create(5, IntegerType.INT);
    List<Expression> exprs = ImmutableList.of(c1, c2, c3, c4, c5);

    Expression res = and(c1, and(c2, and(c3, and(c4, c5))));
    assertEquals(res, PredicateUtils.mergeCNFExpressions(exprs));
  }
}
