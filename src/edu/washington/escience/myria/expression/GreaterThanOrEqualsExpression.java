package edu.washington.escience.myria.expression;

import com.google.common.base.Objects;

import edu.washington.escience.myria.SimplePredicate;
import edu.washington.escience.myria.SimplePredicate.Op;

/**
 * Comparison for greater than or equals in expression tree.
 */
public class GreaterThanOrEqualsExpression extends ComparisonExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private GreaterThanOrEqualsExpression() {
  }

  /**
   * True if left >= right.
   * 
   * @param left the left operand.
   * @param right the right operand.
   */
  public GreaterThanOrEqualsExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  protected Op getOperation() {
    return SimplePredicate.Op.GREATER_THAN_OR_EQ;
  }

  @Override
  public int hashCode() {
    return defaultHashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof GreaterThanOrEqualsExpression)) {
      return false;
    }
    GreaterThanOrEqualsExpression bOther = (GreaterThanOrEqualsExpression) other;
    return Objects.equal(getLeft(), bOther.getLeft()) && Objects.equal(getRight(), bOther.getRight());
  }
}