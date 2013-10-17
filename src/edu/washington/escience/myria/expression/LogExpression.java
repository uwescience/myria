package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Take the natural logarithm (base e) of the operand.
 */
public class LogExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private LogExpression() {
    super();
  }

  /**
   * Take the natural logarithm (base e) of the operand.
   * 
   * @param operand the operand.
   */
  public LogExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final Schema schema) {
    checkAndReturnDefaultNumericType(schema);
    return Type.DOUBLE_TYPE;
  }

  @Override
  public String getJavaString(final Schema schema) {
    return getFunctionCallUnaryString("Math.log", schema);
  }
}