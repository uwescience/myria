package edu.washington.escience.myria.expression;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Exponentiate left^right for two operands in an expression tree. Always evaluates to a double.
 */
public class PowExpression extends BinaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private PowExpression() {
  }

  /**
   * Exponentiate left^right. Always evaluates to a double.
   * 
   * @param left the left operand.
   * @param right the right operand.
   */
  public PowExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  public Type getOutputType(final Schema schema) {
    return Type.DOUBLE_TYPE;
  }

  @Override
  public String getJavaString(final Schema schema) {
    Type leftType = getLeft().getOutputType(schema);
    Type rightType = getRight().getOutputType(schema);
    ImmutableList<Type> validTypes = ImmutableList.of(Type.DOUBLE_TYPE, Type.FLOAT_TYPE, Type.LONG_TYPE, Type.INT_TYPE);
    int leftIdx = validTypes.indexOf(leftType);
    int rightIdx = validTypes.indexOf(rightType);
    Preconditions.checkArgument(leftIdx != -1, "%s cannot handle left child [%s] of Type %s", getClass()
        .getSimpleName(), getLeft(), leftType);
    Preconditions.checkArgument(rightIdx != -1, "%s cannot handle right child [%s] of Type %s", getClass()
        .getSimpleName(), getRight(), rightType);
    return getFunctionCallBinaryString("Math.pow", schema);
  }

  @Override
  public int hashCode() {
    return defaultHashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof PowExpression)) {
      return false;
    }
    PowExpression bOther = (PowExpression) other;
    return Objects.equal(getLeft(), bOther.getLeft()) && Objects.equal(getRight(), bOther.getRight());
  }
}