package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

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
  private ConditionalExpression() {
  }

  /**
   * Create a conditional expression using a ternary operator.
   * 
   * @param condition a boolean expression
   * @param firstChoice the return value if the condition is true
   * @param secondChoice the return value if the condition is false
   */
  public ConditionalExpression(final ExpressionOperator condition, final ExpressionOperator firstChoice,
      final ExpressionOperator secondChoice) {
    super(ImmutableList.of(condition, firstChoice, secondChoice));
  }

  @Override
  public Type getOutputType(final Schema schema, final Schema stateSchema) {
    final Type type = getChild(0).getOutputType(schema, stateSchema);
    Preconditions.checkArgument(type == Type.BOOLEAN_TYPE,
        "%s requires the first child [%s] to be a boolean expression but it is a %s", getClass().getSimpleName(),
        getChild(1), type);

    Type firstType = getChild(1).getOutputType(schema, stateSchema);
    Type secondType = getChild(2).getOutputType(schema, stateSchema);

    if (firstType == secondType) {
      return firstType;
    }

    ImmutableList<Type> validTypes = ImmutableList.of(Type.DOUBLE_TYPE, Type.FLOAT_TYPE, Type.LONG_TYPE, Type.INT_TYPE);
    int firstIdx = validTypes.indexOf(firstType);
    int secondIdx = validTypes.indexOf(secondType);
    Preconditions.checkArgument(firstIdx != -1, "%s cannot handle first choice [%s] of Type %s", getClass()
        .getSimpleName(), getChild(1), firstType);
    Preconditions.checkArgument(secondIdx != -1, "%s cannot handle second choice [%s] of Type %s", getClass()
        .getSimpleName(), getChild(2), secondType);
    return validTypes.get(Math.min(firstIdx, secondIdx));
  }

  @Override
  public String getJavaString(final Schema schema, final Schema stateSchema) {
    return new StringBuilder("(").append(getChild(0).getJavaString(schema, stateSchema)).append("?").append(
        getChild(1).getJavaString(schema, stateSchema)).append(":").append(
        getChild(2).getJavaString(schema, stateSchema)).append(")").toString();
  }
}
