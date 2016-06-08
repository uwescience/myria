package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Take the {@link Math.ceil} of the operand.
 */
public class CeilExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private CeilExpression() {
    super();
  }

  /**
   * Take the {@link Math.ceil} of the operand.
   *
   * @param operand the operand.
   */
  public CeilExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    checkAndReturnDefaultNumericType(parameters);
    return Type.DOUBLE_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return getFunctionCallUnaryString("Math.ceil", parameters);
  }
}
