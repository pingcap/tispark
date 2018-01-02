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

package com.pingcap.tikv.expression;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.pingcap.tikv.expression.scalar.And;
import com.pingcap.tikv.expression.scalar.Not;
import com.pingcap.tikv.expression.scalar.Or;
import org.junit.Test;

public class ExpressionTest {
  @Test
  public void isSupportedExprTest() {
    ExpressionBlacklist blackList = new ExpressionBlacklist("And, , Test");
    Or or = new Or(Constant.create(1), Constant.create(1));
    And and = new And(Constant.create(1), or);
    Not not = new Not(or);
    Not notFail = new Not(and);

    assertTrue(or.isSupportedExpr(blackList));
    assertFalse(and.isSupportedExpr(blackList));
    assertTrue(not.isSupportedExpr(blackList));
    assertFalse(notFail.isSupportedExpr(blackList));
  }
}