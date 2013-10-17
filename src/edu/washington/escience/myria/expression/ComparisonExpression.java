package edu.washington.escience.myria.expression;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.SimplePredicate;
import edu.washington.escience.myria.Type;

/**
 * Comparison in expression tree.
 */
public abstract class ComparisonExpression extends BinaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  protected ComparisonExpression() {
  }

  /**
   * Returns the operation.
   * 
   * @return the operation for this comparison expression
   */
  protected abstract SimplePredicate.Op getOperation();

  /**
   * True if left {@link #getOperation()} right.
   * 
   * @param left the left operand.
   * @param right the right operand.
   */
  public ComparisonExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  public Type getOutputType(final Schema schema) {
    Type leftType = getLeft().getOutputType(schema);
    Type rightType = getRight().getOutputType(schema);

    if (leftType == Type.STRING_TYPE) {
      Preconditions.checkArgument(rightType == Type.STRING_TYPE,
          "If the type of the left child is String, %s requires right child [%s] of Type %s to be String as well.",
          getClass().getSimpleName(), getRight(), rightType);
    } else {
      checkAndReturnDefaultNumericType(schema);
    }
    return Type.BOOLEAN_TYPE;
  }

  @Override
  public String getJavaString(final Schema schema) {
    if (getLeft().getOutputType(schema) == Type.STRING_TYPE) {
      return getObjectComparisonString(getOperation(), schema);
    }
    return getInfixBinaryString(getOperation().toString(), schema);
  }

  @Override
  public int hashCode() {
    return defaultHashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof ComparisonExpression)) {
      return false;
    }
    ComparisonExpression bOther = (ComparisonExpression) other;
    return Objects.equal(getLeft(), bOther.getLeft()) && Objects.equal(getRight(), bOther.getRight());
  }
}