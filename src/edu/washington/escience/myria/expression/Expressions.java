package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.operator.Operator;

/**
 * Utilities to create Expressions.
 */
public final class Expressions {
  /** Utility class cannot be constructed. */
  private Expressions() {}

  /**
   * Construct an expression to select the specified column from the child.
   *
   * @param child the child operator
   * @param column the column to be selected
   * @return an expression to select the specified column from the child
   */
  public static Expression columnSelect(final Operator child, final int column) {
    ExpressionOperator op = new VariableExpression(column);
    return new Expression(child.getSchema().getColumnName(column), op);
  }
}
