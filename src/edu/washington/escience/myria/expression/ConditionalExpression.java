package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * A conditional expression that uses the ternary operator.
 */
public class ConditionalExpression extends NAryExpression {

  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private ConditionalExpression() {}

  /**
   * Create a conditional expression using a ternary operator.
   *
   * @param condition a boolean expression
   * @param firstChoice the return value if the condition is true
   * @param secondChoice the return value if the condition is false
   */
  public ConditionalExpression(
      final ExpressionOperator condition,
      final ExpressionOperator firstChoice,
      final ExpressionOperator secondChoice) {
    super(ImmutableList.of(condition, firstChoice, secondChoice));
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    final Type type = getChild(0).getOutputType(parameters);
    Preconditions.checkArgument(
        type == Type.BOOLEAN_TYPE,
        "%s requires the first child [%s] to be a boolean expression but it is a %s",
        getClass().getSimpleName(),
        getChild(1),
        type);

    Type firstType = getChild(1).getOutputType(parameters);
    Type secondType = getChild(2).getOutputType(parameters);

    if (firstType == secondType) {
      return firstType;
    }

    ImmutableList<Type> validTypes =
        ImmutableList.of(Type.DOUBLE_TYPE, Type.FLOAT_TYPE, Type.LONG_TYPE, Type.INT_TYPE);
    int firstIdx = validTypes.indexOf(firstType);
    int secondIdx = validTypes.indexOf(secondType);
    Preconditions.checkArgument(
        firstIdx != -1,
        "%s cannot handle first choice [%s] of Type %s",
        getClass().getSimpleName(),
        getChild(1),
        firstType);
    Preconditions.checkArgument(
        secondIdx != -1,
        "%s cannot handle second choice [%s] of Type %s",
        getClass().getSimpleName(),
        getChild(2),
        secondType);
    return validTypes.get(Math.min(firstIdx, secondIdx));
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return new StringBuilder("(")
        .append(getChild(0).getJavaString(parameters))
        .append("?")
        .append(getChild(1).getJavaString(parameters))
        .append(":")
        .append(getChild(2).getJavaString(parameters))
        .append(")")
        .toString();
  }
}
