package edu.washington.escience.myria.expression;

import com.google.common.base.Objects;

import edu.washington.escience.myria.SimplePredicate;
import edu.washington.escience.myria.SimplePredicate.Op;

/**
 * Comparison for greater than in expression tree.
 */
public class GreaterThanExpression extends ComparisonExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private GreaterThanExpression() {
  }

  /**
   * True if left > right.
   * 
   * @param left the left operand.
   * @param right the right operand.
   */
  public GreaterThanExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  protected Op getOperation() {
    return SimplePredicate.Op.GREATER_THAN;
  }

  @Override
  public int hashCode() {
    return defaultHashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof GreaterThanExpression)) {
      return false;
    }
    GreaterThanExpression bOther = (GreaterThanExpression) other;
    return Objects.equal(getLeft(), bOther.getLeft()) && Objects.equal(getRight(), bOther.getRight());
  }
}