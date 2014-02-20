package edu.washington.escience.myria.expression;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Modulo of two operands in an expression tree.
 */
public class ModuloExpression extends BinaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private ModuloExpression() {
  }

  /**
   * Add the two operands together.
   * 
   * @param left the left operand.
   * @param right the right operand.
   */
  public ModuloExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  public Type getOutputType(final Schema schema, final Schema stateSchema) {
    return checkAndReturnDefaultNumericType(schema, stateSchema, ImmutableList.of(Type.LONG_TYPE, Type.INT_TYPE));
  }

  @Override
  public String getJavaString(final Schema schema, final Schema stateSchema) {
    return getInfixBinaryString("%", schema, stateSchema);
  }
}