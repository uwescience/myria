package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Take the absolute value of the operand.
 */
public class AbsExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private AbsExpression() {
    super();
  }

  /**
   * Take the absolute value of the operand.
   * 
   * @param operand the operand.
   */
  public AbsExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final Schema schema) {
    return checkAndReturnDefaultNumericType(schema);
  }

  @Override
  public String getJavaString(final Schema schema) {
    return getFunctionCallUnaryString("Math.abs", schema);
  }
}