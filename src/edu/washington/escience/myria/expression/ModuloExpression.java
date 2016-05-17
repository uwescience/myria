package edu.washington.escience.myria.expression;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

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
  private ModuloExpression() {}

  /**
   * Divide the two operands and take the remainder.
   *
   * @param left the left operand.
   * @param right the right operand.
   */
  public ModuloExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return checkAndReturnDefaultNumericType(
        parameters, ImmutableList.of(Type.LONG_TYPE, Type.INT_TYPE));
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return getInfixBinaryString("%", parameters);
  }
}
