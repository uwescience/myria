package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.SimplePredicate;

/**
 * Comparison for less than in expression tree.
 */
public class LessThanExpression extends ComparisonExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is used automagically by Jackson deserialization.
   */
  private LessThanExpression() {
    super(SimplePredicate.Op.LESS_THAN);
  }

  /**
   * True if left < right.
   *
   * @param left the left operand.
   * @param right the right operand.
   */
  public LessThanExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right, SimplePredicate.Op.LESS_THAN);
  }
}
