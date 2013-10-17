package edu.washington.escience.myria.expression;

import com.google.common.base.Objects;

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
    checkBooleanType(schema);
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