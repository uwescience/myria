package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Take the square root of the operand.
 */
public class SqrtExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private SqrtExpression() {
    super();
  }

  /**
   * Take the square root of the operand.
   * 
   * @param operand the operand.
   */
  public SqrtExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final Schema schema) {
    checkAndReturnDefaultNumericType(schema);
    return Type.DOUBLE_TYPE;
  }

  @Override
  public String getJavaString(final Schema schema) {
    return getFunctionCallUnaryString("Math.sqrt", schema);
  }
}