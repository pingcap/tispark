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

import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import java.io.Serializable;

public interface TiExpr extends Serializable {
  Expr toProto();

  default boolean isSupportedExpr(ExpressionBlacklist blackList) {
    if (blackList != null && blackList.isUnsupportedPushdownExpr(getClass())) {
      return false;
    }
    try {
      Expr expr = toProto();
      return expr != null;
    } catch (Exception e) {
      return false;
    }
  }

  DataType getType();

  // TODO: Make it visitor
  TiExpr resolve(TiTableInfo table);
}
