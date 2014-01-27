package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Take the {@link Math.tan} of the operand.
 */
public class TanExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private TanExpression() {
    super();
  }

  /**
   * Take the {@link Math.tan} of the operand.
   * 
   * @param operand the operand.
   */
  public TanExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final Schema schema, final Schema stateSchema) {
    checkAndReturnDefaultNumericType(schema, stateSchema);
    return Type.DOUBLE_TYPE;
  }

  @Override
  public String getJavaString(final Schema schema, final Schema stateSchema) {
    return getFunctionCallUnaryString("Math.tan", schema, stateSchema);
  }
}