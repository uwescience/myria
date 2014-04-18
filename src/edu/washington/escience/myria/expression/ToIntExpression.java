package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Convert from STRING_TYPE to INT_TYPE.
 */
public class ToIntExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private ToIntExpression() {
    super();
  }

  /**
   * Convert from STRING_TYPE to INT_TYPE.
   * 
   * @param operand the operand.
   */
  public ToIntExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    Type operandType = getOperand().getOutputType(parameters);
    Preconditions.checkArgument(operandType == Type.STRING_TYPE, "The operand type must be string.");
    return Type.INT_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return getFunctionCallUnaryString("Integer.parseInt", parameters);
  }

}
