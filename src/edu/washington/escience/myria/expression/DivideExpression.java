package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Divide two operands in an expression tree using floating point arithmetic. This operator returns a
 * {@link Type.DOUBLE_TYPE}.
 */
public class DivideExpression extends BinaryExpression {

  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private DivideExpression() {}

  /**
   * Divide the two operands together.
   *
   * @param left the left operand.
   * @param right the right operand.
   */
  public DivideExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return Type.DOUBLE_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return new StringBuilder("(((double)")
        .append(getLeft().getJavaString(parameters))
        .append(")/")
        .append(getRight().getJavaString(parameters))
        .append(')')
        .toString();
  }
}
