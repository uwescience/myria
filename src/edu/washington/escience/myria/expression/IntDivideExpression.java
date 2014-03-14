package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Divide two operands in an expression tree using integer division. In Myria, this expression always returns a
 * {@link Type.LONG_TYPE}.
 */
public class IntDivideExpression extends BinaryExpression {

  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private IntDivideExpression() {
  }

  /**
   * Divide the two operands together.
   * 
   * @param left the left operand.
   * @param right the right operand.
   */
  public IntDivideExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return Type.LONG_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return new StringBuilder("((long)").append(getInfixBinaryString("/", parameters)).append(')').toString();
  }
}