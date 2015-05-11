package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Return a sequence of numbers from 0 (inclusive) to operand (exclusive).
 */
public class CounterExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private CounterExpression() {
    super();
  }

  /**
   * Takes the upper bound of the sequence to be returned.
   * 
   * @param operand an expression that evaluates to a positive integer
   */
  public CounterExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    Type operandType = getOperand().getOutputType(parameters);
    ImmutableList<Type> validTypes = ImmutableList.of(Type.INT_TYPE);
    int operandIdx = validTypes.indexOf(operandType);
    Preconditions.checkArgument(operandIdx != -1, "%s cannot handle operand [%s] of Type %s", getClass()
        .getSimpleName(), getOperand(), operandType);
    return Type.INT_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    // TODO: use IntStream when we switch to Java 8
    return new StringBuilder().append("int[] counter; for (int i = 0; i < (int) (").append(
        getOperand().getJavaString(parameters)).append("); ++i) {\ncounter[i] = i;\n}\nreturn counter;").toString();
  }

  @Override
  public String getJavaExpressionWithAppend(final ExpressionOperatorParameter parameters) {
    return new StringBuilder().append(Expression.COUNT).append(".appendInt((int) (").append(
        getOperand().getJavaString(parameters)).append("));\nfor (int i = 0; i < (int) (").append(
        getOperand().getJavaString(parameters)).append("); ++i) {\n").append(Expression.RESULT).append(".putInt(")
        .append(Expression.COL).append(", i);\n}").toString();
  }

  @Override
  public boolean hasArrayOutputType() {
    return true;
  }
}
