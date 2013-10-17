package edu.washington.escience.myria.expression;

import com.google.common.base.Objects;

import edu.washington.escience.myria.SimplePredicate;
import edu.washington.escience.myria.SimplePredicate.Op;

/**
 * Comparison for less than or equals in expression tree.
 */
public class LessThanOrEqualsExpression extends ComparisonExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private LessThanOrEqualsExpression() {
  }

  /**
   * True if left <= right.
   * 
   * @param left the left operand.
   * @param right the right operand.
   */
  public LessThanOrEqualsExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  protected Op getOperation() {
    return SimplePredicate.Op.LESS_THAN_OR_EQ;
  }

  @Override
  public int hashCode() {
    return defaultHashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof LessThanOrEqualsExpression)) {
      return false;
    }
    LessThanOrEqualsExpression bOther = (LessThanOrEqualsExpression) other;
    return Objects.equal(getLeft(), bOther.getLeft()) && Objects.equal(getRight(), bOther.getRight());
  }
}