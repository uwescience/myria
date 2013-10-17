package edu.washington.escience.myria.expression;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Multiply two operands in an expression tree.
 */
public class TimesExpression extends BinaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private TimesExpression() {
  }

  /**
   * Multiply the two operands together.
   * 
   * @param left the left operand.
   * @param right the right operand.
   */
  public TimesExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  public Type getOutputType(final Schema schema) {
    Type leftType = getLeft().getOutputType(schema);
    Type rightType = getRight().getOutputType(schema);
    ImmutableList<Type> validTypes = ImmutableList.of(Type.DOUBLE_TYPE, Type.FLOAT_TYPE, Type.LONG_TYPE, Type.INT_TYPE);
    int leftIdx = validTypes.indexOf(leftType);
    int rightIdx = validTypes.indexOf(rightType);
    Preconditions.checkArgument(leftIdx != -1, "TimesExpression cannot handle left child [%s] of Type %s", getLeft(),
        leftType);
    Preconditions.checkArgument(rightIdx != -1, "TimesExpression cannot handle right child [%s] of Type %s",
        getRight(), rightType);
    return validTypes.get(Math.min(leftIdx, rightIdx));
  }

  @Override
  public String getJavaString(final Schema schema) {
    return getInfixBinaryString("*", schema);
  }

  @Override
  public int hashCode() {
    return defaultHashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof TimesExpression)) {
      return false;
    }
    TimesExpression bOther = (TimesExpression) other;
    return Objects.equal(getLeft(), bOther.getLeft()) && Objects.equal(getRight(), bOther.getRight());
  }
}