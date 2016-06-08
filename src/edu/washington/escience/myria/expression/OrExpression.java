package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Boolean or in an expression tree.
 */
public class OrExpression extends BinaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private OrExpression() {}

  /**
   * True if left or right is true.
   *
   * @param left the left operand.
   * @param right the right operand.
   */
  public OrExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    checkBooleanType(parameters);
    return Type.BOOLEAN_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return getInfixBinaryString("||", parameters);
  }
}
