package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Multiply two operands in an expression tree.
 */
public class TimesExpression extends BinaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private TimesExpression() {}

  /**
   * Multiply the two operands together.
   *
   * @param left the left operand.
   * @param right the right operand.
   */
  public TimesExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return checkAndReturnDefaultNumericType(parameters);
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    Type t = getOutputType(parameters);
    if (t == Type.INT_TYPE) {
      return getFunctionCallBinaryString(
          "com.google.common.math.IntMath.checkedMultiply", parameters);
    } else if (t == Type.LONG_TYPE) {
      return getFunctionCallBinaryString(
          "com.google.common.math.LongMath.checkedMultiply", parameters);
    }
    return getInfixBinaryString("*", parameters);
  }
}
