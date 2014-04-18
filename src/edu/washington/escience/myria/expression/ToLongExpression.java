package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Convert from STRING_TYPE to LONG_TYPE.
 * 
 */
public class ToLongExpression extends UnaryExpression {

  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private ToLongExpression() {
    super();
  }

  /**
   * Convert from STRING_TYPE to LONG_TYPE.
   * 
   * @param operand the operand.
   */
  public ToLongExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    Type operandType = getOperand().getOutputType(parameters);
    Preconditions.checkArgument(operandType == Type.STRING_TYPE, "The operand type must be string.");
    return Type.LONG_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return getFunctionCallUnaryString("Long.parseLong", parameters);
  }

}
