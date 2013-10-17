package edu.washington.escience.myria.expression;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Boolean or in an expression tree.
 */
public class OrExpression extends BinaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private OrExpression() {
  }

  /**
   * True if left or right is true.
   *
   * @param left the left operand.
   * @param right the right operand.
   */
  public OrExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  public Type getOutputType(final Schema schema) {
    Type leftType = getLeft().getOutputType(schema);
    Type rightType = getRight().getOutputType(schema);
    ImmutableList<Type> validTypes = ImmutableList.of(Type.BOOLEAN_TYPE);
    int leftIdx = validTypes.indexOf(leftType);
    int rightIdx = validTypes.indexOf(rightType);
    Preconditions.checkArgument(leftIdx != -1, "OrExpression cannot handle left child [%s] of Type %s", getLeft(),
        leftType);
    Preconditions.checkArgument(rightIdx != -1, "OrExpression cannot handle right child [%s] of Type %s", getRight(),
        rightType);
    return Type.BOOLEAN_TYPE;
  }

  @Override
  public String getJavaString(final Schema schema) {
    return getInfixBinaryString("||", schema);
  }

  @Override
  public int hashCode() {
    return defaultHashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof OrExpression)) {
      return false;
    }
    OrExpression bOther = (OrExpression) other;
    return Objects.equal(getLeft(), bOther.getLeft()) && Objects.equal(getRight(), bOther.getRight());
  }
}