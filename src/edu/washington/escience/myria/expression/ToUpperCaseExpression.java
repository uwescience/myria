package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Return the upper case representation of the operand.
 */
public class ToUpperCaseExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private ToUpperCaseExpression() {
    super();
  }

  /**
   * Change all characters in a string to upper case.
   *
   * @param operand the operand.
   */
  public ToUpperCaseExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    Type operandType = getOperand().getOutputType(parameters);
    Preconditions.checkArgument(
        operandType == Type.STRING_TYPE,
        "%s cannot handle operand [%s] of Type %s",
        getClass().getSimpleName(),
        getOperand(),
        operandType);
    return Type.STRING_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return getDotFunctionCallUnaryString(".toUpperCase()", parameters);
  }
}
