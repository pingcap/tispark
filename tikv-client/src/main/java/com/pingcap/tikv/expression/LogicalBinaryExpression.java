// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: expression.proto

package com.pingcap.tikv.expression;


import static java.util.Objects.requireNonNull;

import java.util.List;

public class LogicalBinaryExpression implements TiExpr {

  public static LogicalBinaryExpression and(TiExpr left, TiExpr right) {
    return new LogicalBinaryExpression(Type.AND, left, right);
  }

  public static LogicalBinaryExpression or(TiExpr left, TiExpr right) {
    return new LogicalBinaryExpression(Type.OR, left, right);
  }

  public static LogicalBinaryExpression xor(TiExpr left, TiExpr right) {
    return new LogicalBinaryExpression(Type.XOR, left, right);
  }

  public LogicalBinaryExpression(Type type, TiExpr left, TiExpr right) {
    this.left = requireNonNull(left, "left expression is null");
    this.right = requireNonNull(right, "right expression is null");
    this.compType = requireNonNull(type, "type is null");
  }

  @Override
  public List<TiExpr> getChildren() {
    return null;
  }

  @Override
  public <R, C> R accept(Visitor<R, C> visitor, C context) {
    return null;
  }

  public TiExpr getLeft() {
    return left;
  }

  public TiExpr getRight() {
    return right;
  }

  public Type getCompType() {
    return compType;
  }

  public enum Type {
    AND,
    OR,
    XOR
  }

  private final TiExpr left;
  private final TiExpr right;
  private final Type compType;
}
