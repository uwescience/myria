package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Return the upper case representation of the operand.
 */
public class ToUpperCaseExpression extends UnaryExpression {

  /**
   * Take the square root of the operand.
   * 
   * @param operand the operand.
   */
  public ToUpperCaseExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final Schema schema) {
    Type operandType = getChild().getOutputType(schema);
    ImmutableList<Type> validTypes = ImmutableList.of(Type.STRING_TYPE);
    int operandIdx = validTypes.indexOf(operandType);
    Preconditions.checkArgument(operandIdx != -1, "ToUpperCaseExpression cannot handle operand [%s] of Type %s",
        getChild(), operandType);
    return Type.STRING_TYPE;
  }

  @Override
  public String getJavaString(final Schema schema) {
    return getDotFunctionCallUnaryString(".toUpperCase()", schema);
  }
}