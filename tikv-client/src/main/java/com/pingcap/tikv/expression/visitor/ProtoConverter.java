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


import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.FunctionCall;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataType.EncodeType;

public class ProtoConverter extends DefaultVisitor<Expr, Void> {
  public static Expr toProto(Expression expression) {
    ProtoConverter converter = new ProtoConverter();
    return expression.accept(converter, null);
  }

  @Override
  protected Expr process(Expression node, Void context) {
    Expr.Builder builder = Expr.newBuilder();
    builder.setTp(ExprType.ScalarFunc);

    for (Expression child : node.getChildren()) {
      Expr exprProto = child.accept(this, context);
      builder.addChildren(exprProto);
    }

    return builder.build();
  }

  @Override
  protected Expr visit(ColumnRef node, Void context) {
    Expr.Builder builder = Expr.newBuilder();
    builder.setTp(ExprType.ColumnRef);
    CodecDataOutput cdo = new CodecDataOutput();
    // After switching to DAG request mode, expression value
    // should be the index of table columns we provided in
    // the first executor of a DAG request.
    IntegerCodec.writeLong(cdo, node.getColumnInfo().getOffset());
    builder.setVal(cdo.toByteString());
    return builder.build();
  }

  protected Expr visit(Constant node, Void context) {
    Expr.Builder builder = Expr.newBuilder();
    if (node.getValue() == null) {
      builder.setTp(ExprType.Null);
      return builder.build();
    } else {
      DataType type = node.getType();
      builder.setTp(type.getProtoExprType());
      CodecDataOutput cdo = new CodecDataOutput();
      type.encode(cdo, EncodeType.PROTO, node.getValue());
      builder.setVal(cdo.toByteString());
    }
    return builder.build();
  }

  protected Expr visit(FunctionCall node, Void context) {
    return process(node, context);
  }
}
