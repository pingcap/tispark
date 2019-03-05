package com.pingcap.tikv.expression;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DateTimeType;
import com.pingcap.tikv.types.StringType;
import java.util.List;
import java.util.Objects;
import org.joda.time.DateTime;

public class Year implements Expression {
  private Expression expression;

  public Year(Expression expr) {
    this.expression = expr;
  }

  public static Year year(Expression expr) {
    return new Year(expr);
  }

  @Override
  public List<Expression> getChildren() {
    return ImmutableList.of(expression);
  }

  @Override
  public <R, C> R accept(Visitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  public Expression getExpression() {
    return expression;
  }

  @Override
  public String toString() {
    return String.format("year(%s)", getExpression());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Year)) {
      return false;
    }

    Year that = (Year) other;
    return Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(expression);
  }

  // try to evaluate a {@code Constant} literal if its type is
  // varchar or datetime. If such literal cannot be evaluated, return
  // input literal.
  public Constant eval(Constant literal) {
    DataType type = literal.getType();
    if (type == StringType.VARCHAR) {
      DateTime date = DateTime.parse((String) literal.getValue());
      return Constant.create(date.getYear());
    } else if (type == DateTimeType.DATETIME) {
      DateTime date = (DateTime) literal.getValue();
      return Constant.create(date.getYear());
    } else {
      return literal;
    }
  }
}
