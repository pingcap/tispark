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

package com.pingcap.tikv.expression.visitor;

import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.ExpressionBlacklist;

public class SupportedExpressionValidator extends DefaultVisitor<Boolean, ExpressionBlacklist> {
  private static final SupportedExpressionValidator validator = new SupportedExpressionValidator();

  public static boolean isSupportedExpression(Expression node, ExpressionBlacklist blacklist) {
    if (!node.accept(validator, blacklist)) {
      return false;
    }
    try {
      ExpressionTypeCoercer coercer = new ExpressionTypeCoercer();
      coercer.infer(node);
      ProtoConverter protoConverter = new ProtoConverter(coercer.getTypeMap(), false);
      if (node.accept(protoConverter, null) == null) {
        return false;
      }
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  @Override
  protected Boolean process(Expression node, ExpressionBlacklist blacklist) {
    if (blacklist != null && blacklist.isUnsupportedPushdownExpr(getClass())) {
      return false;
    }
    for (Expression expr : node.getChildren()) {
      if (!expr.accept(this, blacklist)) {
        return false;
      }
    }
    return true;
  }
}
