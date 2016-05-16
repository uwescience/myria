package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.SimplePredicate;

/**
 * Comparison for not equality in expression tree.
 */
public class NotEqualsExpression extends ComparisonExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is used automagically by Jackson deserialization.
   */
  private NotEqualsExpression() {
    super(SimplePredicate.Op.NOT_EQUALS);
  }

  /**
   * True if left != right.
   *
   * @param left the left operand.
   * @param right the right operand.
   */
  public NotEqualsExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right, SimplePredicate.Op.NOT_EQUALS);
  }
}
