package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.SimplePredicate;

/**
 * Comparison for less than in expression tree.
 */
public class LessThanExpression extends ComparisonExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private LessThanExpression() {
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