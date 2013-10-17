package edu.washington.escience.myria.expression;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Negate (boolean not) the operand.
 */
public class NotExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private NotExpression() {
  }

  /**
   * Negate (boolean not) the operand.
   *
   * @param operand the operand.
   */
  public NotExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final Schema schema) {
    Type operandType = getOperand().getOutputType(schema);
    ImmutableList<Type> validTypes = ImmutableList.of(Type.BOOLEAN_TYPE);
    int operandIdx = validTypes.indexOf(operandType);
    Preconditions.checkArgument(operandIdx != -1, "NotExpression cannot handle operand [%s] of Type %s", getOperand(),
        operandType);
    return Type.BOOLEAN_TYPE;
  }

  @Override
  public String getJavaString(final Schema schema) {
    return getFunctionCallUnaryString("!", schema);
  }

  @Override
  public int hashCode() {
    return defaultHashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof NotExpression)) {
      return false;
    }
    NotExpression otherExpr = (NotExpression) other;
    return Objects.equal(getOperand(), otherExpr.getOperand());
  }
}