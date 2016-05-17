package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Return the length of a string.
 *
 */
public class LenExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private LenExpression() {
    super();
  }

  /**
   * Get the lenght of a string.
   *
   * @param operand the operand.
   */
  public LenExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    Type operandType = getOperand().getOutputType(parameters);
    ImmutableList<Type> validTypes = ImmutableList.of(Type.STRING_TYPE);
    int operandIdx = validTypes.indexOf(operandType);
    Preconditions.checkArgument(
        operandIdx != -1,
        "%s cannot handle operand [%s] of Type %s",
        getClass().getSimpleName(),
        getOperand(),
        operandType);
    return Type.INT_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return getDotFunctionCallUnaryString(".length()", parameters);
  }
}
