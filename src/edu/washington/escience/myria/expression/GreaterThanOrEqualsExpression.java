package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.SimplePredicate;

/**
 * Comparison for greater than or equals in expression tree.
 */
public class GreaterThanOrEqualsExpression extends ComparisonExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is used used automagically by Jackson deserialization.
   */
  private GreaterThanOrEqualsExpression() {
    super(SimplePredicate.Op.GREATER_THAN_OR_EQ);
  }

  /**
   * True if left >= right.
   *
   * @param left the left operand.
   * @param right the right operand.
   */
  public GreaterThanOrEqualsExpression(
      final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right, SimplePredicate.Op.GREATER_THAN_OR_EQ);
  }
}
