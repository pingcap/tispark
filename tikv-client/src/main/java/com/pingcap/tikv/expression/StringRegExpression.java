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

import static com.pingcap.tikv.expression.StringRegExpression.Type.CONTAINS;
import static com.pingcap.tikv.expression.StringRegExpression.Type.ENDS_WITH;
import static com.pingcap.tikv.expression.StringRegExpression.Type.LIKE;
import static com.pingcap.tikv.expression.StringRegExpression.Type.STARTS_WITH;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import java.util.List;
import java.util.Objects;

public class StringRegExpression extends Expression {
  private final Expression left;
  private final Expression right;
  private final Expression reg;
  private final Type regType;
  private transient TypedKey key;

  public StringRegExpression(Type type, Expression left, Expression right, Expression reg) {
    super(IntegerType.BOOLEAN);
    resolved = true;
    this.left = requireNonNull(left, "left expression is null");
    this.right = requireNonNull(right, "right expression is null");
    this.regType = requireNonNull(type, "type is null");
    this.reg = requireNonNull(reg, "reg string is null");
  }

  public static StringRegExpression startsWith(Expression left, Expression right) {
    Expression reg = Constant.create(((Constant) right).getValue() + "%", right.getDataType());
    return new StringRegExpression(STARTS_WITH, left, right, reg);
  }

  public static StringRegExpression contains(Expression left, Expression right) {
    Expression reg =
        Constant.create("%" + ((Constant) right).getValue() + "%", right.getDataType());
    return new StringRegExpression(CONTAINS, left, right, reg);
  }

  public static StringRegExpression endsWith(Expression left, Expression right) {
    Expression reg = Constant.create("%" + ((Constant) right).getValue(), right.getDataType());
    return new StringRegExpression(ENDS_WITH, left, right, reg);
  }

  public static StringRegExpression like(Expression left, Expression right) {
    return new StringRegExpression(LIKE, left, right, right);
  }

  public ColumnRef getColumnRef() {
    return (ColumnRef) getLeft();
  }

  public Constant getValue() {
    return (Constant) getRight();
  }

  public TypedKey getTypedLiteral() {
    return getTypedLiteral(DataType.UNSPECIFIED_LEN);
  }

  public TypedKey getTypedLiteral(int prefixLength) {
    if (key == null) {
      key = TypedKey.toTypedKey(getValue().getValue(), getColumnRef().getDataType(), prefixLength);
    }
    return key;
  }

  @Override
  public List<Expression> getChildren() {
    // For LIKE statement, an extra ESCAPE parameter is required as the third parameter for
    // ScalarFunc.
    // However in Spark ESCAPE is not supported so we simply set this value to zero.
    return ImmutableList.of(left, reg, Constant.create(0, IntegerType.BIGINT));
  }

  @Override
  public <R, C> R accept(Visitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  public Expression getLeft() {
    return left;
  }

  public Expression getRight() {
    return right;
  }

  public Type getRegType() {
    return regType;
  }

  public Expression getReg() {
    return reg;
  }

  @Override
  public String toString() {
    return String.format("[%s %s %s reg: %s]", getLeft(), getRegType(), getRight(), getReg());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof StringRegExpression)) {
      return false;
    }

    StringRegExpression that = (StringRegExpression) other;
    return (regType == that.regType)
        && Objects.equals(left, that.left)
        && Objects.equals(right, that.right)
        && Objects.equals(reg, that.reg);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regType, left, right, reg);
  }

  public enum Type {
    STARTS_WITH,
    CONTAINS,
    ENDS_WITH,
    LIKE
  }
}
