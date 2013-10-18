package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.SimplePredicate;

/**
 * Comparison for equality in expression tree.
 */
public class EqualsExpression extends ComparisonExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private EqualsExpression() {
  }

  /**
   * True if left == right.
   * 
   * @param left the left operand.
   * @param right the right operand.
   */
  public EqualsExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right, SimplePredicate.Op.LIKE);
  }
}