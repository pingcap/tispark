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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.scalar.And;
import java.util.List;
import org.junit.Test;

public class PredicateUtilsTest {
  @Test
  public void mergeCNFExpressions() throws Exception {
    List<Expression> exprs =
        ImmutableList.of(
            Constant.create(1),
            Constant.create(2),
            Constant.create(3),
            Constant.create(4),
            Constant.create(5));

    Expression res =
        new And(
            Constant.create(1),
            new And(
                Constant.create(2),
                new And(
                    Constant.create(3), new And(Constant.create(4), Constant.create(5)))));

    assertEquals(res, PredicateUtils.mergeCNFExpressions(exprs));
  }
}
