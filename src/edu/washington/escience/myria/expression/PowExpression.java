package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Exponentiate left^right for two operands in an expression tree. Always evaluates to a double.
 */
public class PowExpression extends BinaryExpression {

  /**
   * Exponentiate left^right. Always evaluates to a double.
   * 
   * @param left the left operand.
   * @param right the right operand.
   */
  public PowExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  public Type getOutputType(final Schema schema) {
    return Type.DOUBLE_TYPE;
  }

  @Override
  public String getJavaString(final Schema schema) {
    return getFunctionCallBinaryString("Math.pow", schema);
  }
}