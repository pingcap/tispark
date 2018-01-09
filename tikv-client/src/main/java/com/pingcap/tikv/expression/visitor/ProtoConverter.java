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


import com.google.common.collect.ImmutableMap;
import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tidb.tipb.FieldType;
import com.pingcap.tidb.tipb.ScalarFuncSig;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.expression.ArithmeticBinaryExpression;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.FunctionCall;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.types.BitType;
import com.pingcap.tikv.types.BytesType;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataType.EncodeType;
import com.pingcap.tikv.types.DateTimeType;
import com.pingcap.tikv.types.DateType;
import com.pingcap.tikv.types.DecimalType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.RealType;
import com.pingcap.tikv.types.StringType;
import com.pingcap.tikv.types.TimestampType;
import java.util.IdentityHashMap;
import java.util.Map;

public class ProtoConverter extends DefaultVisitor<Expr, Void> {
  // All concrete data type should be hooked to a type name
  private static final Map<Class<? extends DataType>, String> SCALAR_SIG_MAP =
      ImmutableMap.<Class<? extends DataType>, String>builder()
          .put(IntegerType.class, "Int")
          .put(BitType.class, "Int")
          .put(DecimalType.class, "Decimal")
          .put(RealType.class, "Real")
          .put(DateTimeType.class, "Time")
          .put(DateType.class, "Time")
          .put(TimestampType.class, "Time")
          .put(BytesType.class, "String")
          .put(StringType.class, "String")
          .build();

  private final IdentityHashMap<Expression, DataType> typeMap = new IdentityHashMap<>();

  private DataType getType(Expression expression) {
    DataType type = typeMap.get(expression);
    if (type == null) {
      throw new TiExpressionException(String.format("Expression %s type unknown", expression));
    }
    return type;
  }

  private String getTypeSignature(Expression expression) {
    DataType type = getType(expression);
    String typeSignature = SCALAR_SIG_MAP.get(type);
    if (typeSignature == null) {
      throw new TiExpressionException(String.format("Type %s signature unknown", type));
    }
    return typeSignature;
  }

  public static Expr toProto(Expression expression) {
    ProtoConverter converter = new ProtoConverter();
    return expression.accept(converter, null);
  }

  // Generate protobuf builder with partial data encoded.
  // Scala Signature is left alone
  private Expr.Builder scalaToPartialProto(Expression node, Void context) {
    Expr.Builder builder = Expr.newBuilder();
    // Scalar function type
    builder.setTp(ExprType.ScalarFunc);

    // Return type
    builder.setFieldType(
        FieldType.newBuilder()
            .setTp(getType(node).getTypeCode())
            .build());

    for (Expression child : node.getChildren()) {
      Expr exprProto = child.accept(this, context);
      builder.addChildren(exprProto);
    }

    return builder;
  }

  @Override
  protected Expr visit(LogicalBinaryExpression node, Void context) {
    ScalarFuncSig protoSig;
    switch (node.getCompType()) {
      case AND:
        protoSig = ScalarFuncSig.LogicalAnd;
        break;
      case OR:
        protoSig = ScalarFuncSig.LogicalOr;
        break;
      case XOR:
        protoSig = ScalarFuncSig.LogicalXor;
        break;
      default:
        throw new TiExpressionException(String.format("Unknown comparison type %s", node.getCompType()));
    }
    Expr.Builder builder = scalaToPartialProto(node, context);
    builder.setSig(protoSig);
    return builder.build();
  }

  protected Expr visit(ArithmeticBinaryExpression node, Void context) {
    String typeSignature = getTypeSignature(node);
    ScalarFuncSig protoSig;
    switch (node.getCompType()) {
      // TODO: Add test for bitwise push down
      case BIT_AND:
        protoSig = ScalarFuncSig.BitAndSig;
        break;
      case BIT_OR:
        protoSig = ScalarFuncSig.BitOrSig;
        break;
      case BIT_XOR:
        protoSig = ScalarFuncSig.BitXorSig;
        break;
      case DIVIDE:
        protoSig = ScalarFuncSig.valueOf("Divide" + typeSignature);
        break;
      case MINUS:
        protoSig = ScalarFuncSig.valueOf("Minus" + typeSignature);
        break;
      case MULTIPLY:
        protoSig = ScalarFuncSig.valueOf("Multiply" + typeSignature);
        break;
      case PLUS:
        protoSig = ScalarFuncSig.valueOf("Plus" + typeSignature);
        break;
      default:
        throw new TiExpressionException(String.format("Unknown comparison type %s", node.getCompType()));
    }
    Expr.Builder builder = scalaToPartialProto(node, context);
    builder.setSig(protoSig);
    return builder.build();
  }

  @Override
  protected Expr visit(ComparisonBinaryExpression node, Void context) {
    String typeSignature = getTypeSignature(node);
    ScalarFuncSig protoSig;
    switch (node.getComparisonType()) {
      case EQUAL:
        protoSig = ScalarFuncSig.valueOf("EQ" + typeSignature);
        break;
      case GREATER_EQUAL:
        protoSig = ScalarFuncSig.valueOf("GE" + typeSignature);
        break;
      case GREATER_THAN:
        protoSig = ScalarFuncSig.valueOf("GT" + typeSignature);
        break;
      case LESS_EQUAL:
        protoSig = ScalarFuncSig.valueOf("LE" + typeSignature);
        break;
      case LESS_THAN:
        protoSig = ScalarFuncSig.valueOf("LT" + typeSignature);
        break;
      case NOT_EQUAL:
        protoSig = ScalarFuncSig.valueOf("NE" + typeSignature);
        break;
      default:
        throw new TiExpressionException(String.format("Unknown comparison type %s", node.getComparisonType()));
    }
    Expr.Builder builder = scalaToPartialProto(node, context);
    builder.setSig(protoSig);
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
