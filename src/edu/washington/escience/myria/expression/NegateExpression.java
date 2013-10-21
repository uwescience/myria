package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Negate (Unary minus) the operand.
 */
public class NegateExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private NegateExpression() {
  }

  /**
   * Negate (unary minus) the operand.
   * 
   * @param operand the operand.
   */
  public NegateExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final Schema schema) {
    return checkAndReturnDefaultNumericType(schema);
  }

  @Override
  public String getJavaString(final Schema schema) {
    return getFunctionCallUnaryString("-", schema);
  }
}