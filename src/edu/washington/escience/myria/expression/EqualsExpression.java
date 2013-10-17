package edu.washington.escience.myria.expression;

import com.google.common.base.Objects;

import edu.washington.escience.myria.SimplePredicate;
import edu.washington.escience.myria.SimplePredicate.Op;

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
    super(left, right);
  }

  @Override
  protected Op getOperation() {
    return SimplePredicate.Op.LIKE;
  }

  @Override
  public int hashCode() {
    return defaultHashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof EqualsExpression)) {
      return false;
    }
    EqualsExpression bOther = (EqualsExpression) other;
    return Objects.equal(getLeft(), bOther.getLeft()) && Objects.equal(getRight(), bOther.getRight());
  }
}