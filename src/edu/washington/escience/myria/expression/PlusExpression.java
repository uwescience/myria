package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Add two operands in an expression tree.
 */
public class PlusExpression extends BinaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private PlusExpression() {
  }

  /**
   * Add the two operands together.
   * 
   * @param left the left operand.
   * @param right the right operand.
   */
  public PlusExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  public Type getOutputType(final Schema schema, final Schema stateSchema) {
    return checkAndReturnDefaultNumericType(schema, stateSchema);
  }

  @Override
  public String getJavaString(final Schema schema, final Schema stateSchema) {
    return getInfixBinaryString("+", schema, stateSchema);
  }
}