package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Return the lower case representation of the operand.
 */
public class ToLowerCaseExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private ToLowerCaseExpression() {
    super();
  }

  /**
   * Change all characters in a string to upper case.
   * 
   * @param operand the operand.
   */
  public ToLowerCaseExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    Type operandType = getOperand().getOutputType(parameters);
    ImmutableList<Type> validTypes = ImmutableList.of(Type.STRING_TYPE);
    int operandIdx = validTypes.indexOf(operandType);
    Preconditions.checkArgument(operandIdx != -1, "%s cannot handle operand [%s] of Type %s", getClass()
        .getSimpleName(), getOperand(), operandType);
    return Type.STRING_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return getDotFunctionCallUnaryString(".toLowerCase()", parameters);
  }

  @Override
  public String getSqlString(final ExpressionOperatorParameter parameters) {
    return getSqlFunctionCallUnaryString("lower", parameters);
  }
}