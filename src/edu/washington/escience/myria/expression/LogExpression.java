package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

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
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    checkAndReturnDefaultNumericType(parameters);
    return Type.DOUBLE_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return getFunctionCallUnaryString("Math.log", parameters);
  }
}
