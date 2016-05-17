package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.SimplePredicate;

/**
 * Comparison for greater than in expression tree.
 */
public class GreaterThanExpression extends ComparisonExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is used automagically by Jackson deserialization.
   */
  private GreaterThanExpression() {
    super(SimplePredicate.Op.GREATER_THAN);
  }

  /**
   * True if left > right.
   *
   * @param left the left operand.
   * @param right the right operand.
   */
  public GreaterThanExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right, SimplePredicate.Op.GREATER_THAN);
  }
}
