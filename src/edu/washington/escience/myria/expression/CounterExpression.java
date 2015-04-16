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
    ImmutableList<Type> validTypes = ImmutableList.of(Type.LONG_TYPE, Type.INT_TYPE);
    int operandIdx = validTypes.indexOf(operandType);
    Preconditions.checkArgument(operandIdx != -1, "%s cannot handle operand [%s] of Type %s", getClass()
        .getSimpleName(), getOperand(), operandType);
    return Type.LONG_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return new StringBuilder().append(
        "com.google.common.collect.ContiguousSet.create(com.google.common.collect.Range.closedOpen(0L, ").append(
        getOperand().getJavaString(parameters)).append("), com.google.common.collect.DiscreteDomain.longs())")
        .toString();
  }

  @Override
  public boolean hasIterableOutputType() {
    return true;
  }
}
