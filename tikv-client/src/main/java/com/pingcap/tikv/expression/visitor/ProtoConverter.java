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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tidb.tipb.FieldType;
import com.pingcap.tidb.tipb.ScalarFuncSig;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.expression.*;
import com.pingcap.tikv.expression.AggregateFunction.FunctionType;
import com.pingcap.tikv.types.*;
import com.pingcap.tikv.types.DataType.EncodeType;
import java.util.IdentityHashMap;
import java.util.Map;

public class ProtoConverter extends Visitor<Expr, Object> {
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

  private final IdentityHashMap<Expression, DataType> typeMap;
  private final boolean validateColPosition;

  public ProtoConverter(IdentityHashMap<Expression, DataType> typeMap) {
    this(typeMap, true);
  }

  /**
   * Instantiate a {{@code ProtoConverter}} using a typeMap.
   *
   * @param typeMap the type map
   * @param validateColPosition whether to consider column position in this converter. By default, a
   *     {{@code TiDAGRequest}} should check whether a {{@code ColumnRef}}'s position is correct in
   *     it's executors. Can ignore this validation if `validateColPosition` is set to false.
   */
  public ProtoConverter(
      IdentityHashMap<Expression, DataType> typeMap, boolean validateColPosition) {
    this.typeMap = typeMap;
    this.validateColPosition = validateColPosition;
  }

  private DataType getType(Expression expression) {
    DataType type = typeMap.get(expression);
    if (type == null) {
      throw new TiExpressionException(String.format("Expression %s type unknown", expression));
    }
    return type;
  }

  private String getTypeSignature(Expression expression) {
    DataType type = getType(expression);
    String typeSignature = SCALAR_SIG_MAP.get(type.getClass());
    if (typeSignature == null) {
      throw new TiExpressionException(String.format("Type %s signature unknown", type));
    }
    return typeSignature;
  }

  public static Expr toProto(Expression expression) {
    return toProto(expression, null);
  }

  public static Expr toProto(Expression expression, Object context) {
    ExpressionTypeCoercer coercer = new ExpressionTypeCoercer();
    coercer.infer(expression);
    ProtoConverter converter = new ProtoConverter(coercer.getTypeMap());
    return expression.accept(converter, context);
  }

  // Generate protobuf builder with partial data encoded.
  // Scala Signature is left alone
  private Expr.Builder scalaToPartialProto(Expression node, Object context) {
    Expr.Builder builder = Expr.newBuilder();
    // Scalar function type
    builder.setTp(ExprType.ScalarFunc);

    // Return type
    builder.setFieldType(FieldType.newBuilder().setTp(getType(node).getTypeCode()).build());

    for (Expression child : node.getChildren()) {
      Expr exprProto = child.accept(this, context);
      builder.addChildren(exprProto);
    }

    return builder;
  }

  @Override
  protected Expr visit(LogicalBinaryExpression node, Object context) {
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
        throw new TiExpressionException(
            String.format("Unknown comparison type %s", node.getCompType()));
    }
    Expr.Builder builder = scalaToPartialProto(node, context);
    builder.setSig(protoSig);
    return builder.build();
  }

  @Override
  protected Expr visit(ArithmeticBinaryExpression node, Object context) {
    // assume after type coerce, children should be compatible
    Expression child = node.getLeft();
    String typeSignature = getTypeSignature(child);
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
        throw new TiExpressionException(
            String.format("Unknown comparison type %s", node.getCompType()));
    }
    Expr.Builder builder = scalaToPartialProto(node, context);
    builder.setSig(protoSig);
    return builder.build();
  }

  @Override
  protected Expr visit(ComparisonBinaryExpression node, Object context) {
    // assume after type coerce, children should be compatible
    Expression child = node.getLeft();
    String typeSignature = getTypeSignature(child);
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
        throw new TiExpressionException(
            String.format("Unknown comparison type %s", node.getComparisonType()));
    }
    Expr.Builder builder = scalaToPartialProto(node, context);
    builder.setSig(protoSig);
    return builder.build();
  }

  @Override
  protected Expr visit(StringRegExpression node, Object context) {
    // assume after type coerce, children should be compatible
    ScalarFuncSig protoSig;
    switch (node.getRegType()) {
      case STARTS_WITH:
      case CONTAINS:
      case ENDS_WITH:
      case LIKE:
        protoSig = ScalarFuncSig.LikeSig;
        break;
      default:
        throw new TiExpressionException(String.format("Unknown reg type %s", node.getRegType()));
    }
    Expr.Builder builder = scalaToPartialProto(node, context);
    builder.setSig(protoSig);
    return builder.build();
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Expr visit(ColumnRef node, Object context) {
    long position = 0;
    if (validateColPosition) {
      requireNonNull(context, "Context of a ColumnRef should not be null");
      Map<ColumnRef, Integer> colIdOffsetMap = (Map<ColumnRef, Integer>) context;
      position =
          requireNonNull(
              colIdOffsetMap.get(node), "Required column position info is not in a valid context.");
    }
    Expr.Builder builder = Expr.newBuilder();
    builder.setTp(ExprType.ColumnRef);
    CodecDataOutput cdo = new CodecDataOutput();
    // After switching to DAG request mode, expression value
    // should be the index of table columns we provided in
    // the first executor of a DAG request.
    IntegerCodec.writeLong(cdo, position);
    builder.setVal(cdo.toByteString());
    return builder.build();
  }

  @Override
  protected Expr visit(Constant node, Object context) {
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

  @Override
  protected Expr visit(AggregateFunction node, Object context) {
    Expr.Builder builder = Expr.newBuilder();

    FunctionType type = node.getType();
    switch (type) {
      case Max:
        builder.setTp(ExprType.Max);
        break;
      case Sum:
        builder.setTp(ExprType.Sum);
        break;
      case Min:
        builder.setTp(ExprType.Min);
        break;
      case First:
        builder.setTp(ExprType.First);
        break;
      case Count:
        builder.setTp(ExprType.Count);
        break;
    }

    for (Expression arg : node.getChildren()) {
      Expr exprProto = arg.accept(this, context);
      builder.addChildren(exprProto);
    }

    return builder.build();
  }

  @Override
  protected Expr visit(IsNull node, Object context) {
    String typeSignature = getTypeSignature(node.getExpression());
    ScalarFuncSig protoSig = ScalarFuncSig.valueOf(typeSignature + "IsNull");
    Expr.Builder builder = scalaToPartialProto(node, context);
    builder.setSig(protoSig);
    return builder.build();
  }

  @Override
  protected Expr visit(Not node, Object context) {
    ScalarFuncSig protoSig = ScalarFuncSig.UnaryNot;
    Expr.Builder builder = scalaToPartialProto(node, context);
    builder.setSig(protoSig);
    return builder.build();
  }
}
